import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

// Initialize the Apify SDK
await Actor.init();

/**
 * Extract lawyers from JSON-LD structured data (Primary method)
 * Avvo may use Attorney or LegalService schema markup
 */
async function extractLawyersViaJsonLD(page) {
    log.info('Attempting to extract lawyers via JSON-LD');

    try {
        const jsonLdScripts = await page.$$eval('script[type="application/ld+json"]', scripts =>
            scripts.map(script => script.textContent)
        );

        const lawyers = [];

        for (const scriptContent of jsonLdScripts) {
            try {
                const data = JSON.parse(scriptContent);

                // Handle array of attorney listings
                if (Array.isArray(data)) {
                    for (const item of data) {
                        if (item['@type'] === 'Attorney' || item['@type'] === 'Person' || item['@type'] === 'LegalService') {
                            lawyers.push(parseAttorneySchema(item));
                        }
                    }
                }
                // Handle single attorney
                else if (data['@type'] === 'Attorney' || data['@type'] === 'Person' || data['@type'] === 'LegalService') {
                    lawyers.push(parseAttorneySchema(data));
                }
                // Handle @graph structure
                else if (data['@graph']) {
                    for (const item of data['@graph']) {
                        if (item['@type'] === 'Attorney' || item['@type'] === 'Person' || item['@type'] === 'LegalService') {
                            lawyers.push(parseAttorneySchema(item));
                        }
                    }
                }
                // Handle ItemList with attorneys
                else if (data['@type'] === 'ItemList' && data.itemListElement) {
                    for (const listItem of data.itemListElement) {
                        const item = listItem.item || listItem;
                        if (item['@type'] === 'Attorney' || item['@type'] === 'Person' || item['@type'] === 'LegalService') {
                            lawyers.push(parseAttorneySchema(item));
                        }
                    }
                }
            } catch (parseErr) {
                log.debug(`Failed to parse JSON-LD: ${parseErr.message}`);
            }
        }

        if (lawyers.length > 0) {
            log.info(`Extracted ${lawyers.length} lawyers via JSON-LD`);
        }

        return lawyers;
    } catch (error) {
        log.warning(`JSON-LD extraction failed: ${error.message}`);
        return [];
    }
}

/**
 * Parse Attorney schema to our format
 */
function parseAttorneySchema(attorneyData) {
    const address = attorneyData.address || {};
    
    let location = '';
    if (typeof address === 'string') {
        location = address;
    } else {
        location = [
            address.addressLocality,
            address.addressRegion,
            address.postalCode
        ].filter(Boolean).join(', ');
    }

    const practiceAreas = Array.isArray(attorneyData.knowsAbout) 
        ? attorneyData.knowsAbout 
        : (attorneyData.areaServed ? [attorneyData.areaServed] : []);

    return {
        name: attorneyData.name || '',
        rating: attorneyData.aggregateRating?.ratingValue || null,
        reviewCount: attorneyData.aggregateRating?.reviewCount || 0,
        practiceAreas,
        location,
        phone: attorneyData.telephone || '',
        email: attorneyData.email || '',
        website: attorneyData.url || '',
        yearsLicensed: null,
        barAdmissions: [],
        languages: [],
        profileUrl: attorneyData.url || '',
        bio: attorneyData.description || '',
        scrapedAt: new Date().toISOString()
    };
}

/**
 * Fetch lawyer profile details from profile page using got-scraping
 * This is faster than using Playwright for each detail page
 */
async function fetchLawyerProfile(profileUrl, cookies = '', userAgent = '') {
    try {
        const response = await gotScraping({
            url: profileUrl,
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cookie': cookies,
                'User-Agent': userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
            },
            timeout: { request: 15000 },
            retry: { limit: 1 },
        });

        if (response.statusCode === 403 || response.statusCode === 503) {
            log.debug(`Cloudflare block detected on profile page (${response.statusCode}): ${profileUrl}`);
            return { blocked: true };
        }

        if (response.statusCode !== 200) {
            log.debug(`Profile page returned status ${response.statusCode}: ${profileUrl}`);
            return null;
        }

        const $ = cheerio.load(response.body);

        const title = $('title').text();
        if (title.includes('Just a moment') || title.includes('Cloudflare')) {
            log.debug(`Cloudflare challenge page detected: ${profileUrl}`);
            return { blocked: true };
        }

        // Extract additional profile details
        const additionalInfo = {
            bio: $('[data-testid="bio"], .lawyer-bio, .bio-text, .profile-bio').first().text().trim() || '',
            education: [],
            awards: []
        };

        // Extract education
        $('[data-testid="education"] li, .education-item, .school-item, [class*="education"] li').each((_, el) => {
            const education = $(el).text().trim();
            if (education) additionalInfo.education.push(education);
        });

        // Extract awards
        $('[data-testid="awards"] li, .award-item, [class*="award"] li').each((_, el) => {
            const award = $(el).text().trim();
            if (award) additionalInfo.awards.push(award);
        });

        return additionalInfo;

    } catch (error) {
        if (error.message && (error.message.includes('403') || error.message.includes('503'))) {
            log.debug(`Cloudflare block detected (error): ${profileUrl}`);
            return { blocked: true };
        }
        log.debug(`Failed to fetch profile page ${profileUrl}: ${error.message}`);
        return null;
    }
}

/**
 * Enrich lawyers with full profile data from detail pages
 */
async function enrichLawyersWithProfiles(lawyers, page, maxConcurrency = 10) {
    if (lawyers.length === 0) return lawyers;

    log.info(`Fetching full profiles for ${lawyers.length} lawyers...`);

    const cookies = await page.context().cookies();
    const cookieString = cookies.map(c => `${c.name}=${c.value}`).join('; ');
    const userAgent = await page.evaluate(() => navigator.userAgent);

    log.debug(`Using ${cookies.length} cookies from Camoufox session for profile pages`);

    const enrichedLawyers = [];
    const batchSize = maxConcurrency;
    let blockedCount = 0;

    for (let i = 0; i < lawyers.length; i += batchSize) {
        const batch = lawyers.slice(i, i + batchSize);

        const batchPromises = batch.map(async (lawyer) => {
            if (!lawyer.profileUrl) return lawyer;

            const profileData = await fetchLawyerProfile(lawyer.profileUrl, cookieString, userAgent);

            if (profileData && profileData.blocked) {
                blockedCount++;
                log.warning(`Profile page blocked by Cloudflare: ${lawyer.profileUrl}`);
                return lawyer;
            }

            if (profileData) {
                return {
                    ...lawyer,
                    bio: profileData.bio || lawyer.bio,
                    education: profileData.education || [],
                    awards: profileData.awards || []
                };
            }

            return lawyer;
        });

        const batchResults = await Promise.all(batchPromises);
        enrichedLawyers.push(...batchResults);

        log.info(`Enriched ${Math.min(i + batchSize, lawyers.length)}/${lawyers.length} lawyers with full profiles`);

        if (i + batchSize < lawyers.length) {
            await new Promise(resolve => setTimeout(resolve, 200));
        }
    }

    if (blockedCount > 0) {
        log.warning(`${blockedCount} profile pages were blocked by Cloudflare - using basic info instead`);
    }

    return enrichedLawyers;
}

/**
 * Extract API endpoint calls from page network requests
 * Monitor XHR/Fetch requests to find internal API
 */
async function extractLawyersViaAPI(page) {
    log.info('Attempting to extract lawyers via internal API');

    const capturedLawyers = [];

    try {
        const responses = await page.evaluate(async () => {
            const scripts = document.querySelectorAll('script:not([src])');
            const potentialData = [];

            scripts.forEach(script => {
                const content = script.textContent || '';
                if (content.includes('"lawyers"') || content.includes('"attorneys"') ||
                    content.includes('"profiles"') || content.includes('lawyerData')) {
                    potentialData.push(content);
                }
            });

            return potentialData;
        });

        for (const content of responses) {
            try {
                const jsonMatch = content.match(/\{[\s\S]*\}/);
                if (jsonMatch) {
                    const data = JSON.parse(jsonMatch[0]);
                    const lawyerArray = data.lawyers || data.attorneys || data.profiles ||
                        data.data?.lawyers || data.data?.attorneys || [];

                    if (Array.isArray(lawyerArray) && lawyerArray.length > 0) {
                        log.info(`Found ${lawyerArray.length} lawyers in embedded API data`);
                        for (const lawyer of lawyerArray) {
                            capturedLawyers.push({
                                name: lawyer.name || lawyer.fullName || '',
                                rating: lawyer.rating || lawyer.avvoRating || null,
                                reviewCount: lawyer.reviewCount || lawyer.reviews?.length || 0,
                                practiceAreas: lawyer.practiceAreas || lawyer.specialties || [],
                                location: lawyer.location || lawyer.city || '',
                                phone: lawyer.phone || lawyer.phoneNumber || '',
                                email: lawyer.email || '',
                                website: lawyer.website || lawyer.websiteUrl || '',
                                yearsLicensed: lawyer.yearsLicensed || lawyer.yearAdmitted || null,
                                barAdmissions: lawyer.barAdmissions || [],
                                languages: lawyer.languages || [],
                                profileUrl: lawyer.profileUrl || lawyer.url || '',
                                bio: lawyer.bio || lawyer.description || '',
                                scrapedAt: new Date().toISOString()
                            });
                        }
                    }
                }
            } catch (parseErr) {
                // Continue to next script
            }
        }

        return capturedLawyers;

    } catch (error) {
        log.warning(`Failed to capture API data: ${error.message}`);
        return [];
    }
}

/**
 * Build Avvo search URL from input parameters
 */
function buildSearchUrl(input) {
    if (input.startUrl && input.startUrl.trim()) {
        log.info('Using provided start URL directly');
        return input.startUrl.trim();
    }

    const practiceArea = input.practiceArea || 'bankruptcy-debt';
    const state = input.state || 'al';
    const city = input.city ? `${input.city.toLowerCase()}-` : '';

    const baseUrl = `https://www.avvo.com/${practiceArea}-lawyer/${city}${state}.html`;

    log.info(`Built search URL: ${baseUrl}`);
    return baseUrl;
}

/**
 * Extract lawyer data from the page using Cheerio HTML parsing
 */
async function extractLawyerDataViaHTML(page) {
    log.info('Extracting lawyer data via HTML parsing with Cheerio');

    try {
        const html = await page.content();
        const $ = cheerio.load(html);
        const lawyers = [];

        // Try multiple selector strategies for Avvo's lawyer cards
        const selectors = [
            'div[data-testid="lawyer-card"]',
            '.lawyer-card',
            '[class*="lawyer"][class*="card"]',
            'article[data-lawyer-id]',
            '.search-result-lawyer',
            '.profile-card',
            '[data-lawyer-name]'
        ];

        let lawyerElements = $([]);

        for (const selector of selectors) {
            const elements = $(selector);
            if (elements.length > 0) {
                log.info(`Found ${elements.length} lawyer cards with selector: ${selector}`);
                lawyerElements = elements;
                break;
            }
        }

        if (lawyerElements.length === 0) {
            log.warning('No lawyer cards found with standard selectors, trying fallback approach');
            return [];
        }

        lawyerElements.each((_, element) => {
            const lawyer = extractLawyerFromElement($, $(element));
            if (lawyer) lawyers.push(lawyer);
        });

        log.info(`Extracted ${lawyers.length} lawyers via HTML parsing`);
        return lawyers;

    } catch (error) {
        log.warning(`HTML parsing failed: ${error.message}`);
        return [];
    }
}

/**
 * Extract lawyer data from a single lawyer element
 */
function extractLawyerFromElement($, $el) {
    try {
        const nameSelectors = [
            '[data-testid="lawyer-name"]',
            'h2 a',
            'h3 a',
            '.lawyer-name',
            '.profile-name',
            'a[href*="/attorney/"]'
        ];

        let name = '';
        let profileUrl = '';

        for (const selector of nameSelectors) {
            const nameEl = $el.find(selector).first();
            if (nameEl.length && nameEl.text().trim()) {
                name = nameEl.text().trim();
                profileUrl = nameEl.attr('href') || '';
                if (profileUrl && !profileUrl.startsWith('http')) {
                    profileUrl = `https://www.avvo.com${profileUrl}`;
                }
                break;
            }
        }

        const ratingSelectors = [
            '[data-testid="rating"]',
            '.rating-value',
            '.avvo-rating',
            '[class*="rating"]'
        ];

        let rating = null;
        for (const selector of ratingSelectors) {
            const ratingEl = $el.find(selector).first();
            if (ratingEl.length) {
                const ratingText = ratingEl.text().trim();
                const ratingMatch = ratingText.match(/(\d+\.?\d*)/);
                if (ratingMatch) {
                    rating = parseFloat(ratingMatch[1]);
                    break;
                }
            }
        }

        const reviewSelectors = [
            '[data-testid="review-count"]',
            '.review-count',
            '[class*="review"]'
        ];

        let reviewCount = 0;
        for (const selector of reviewSelectors) {
            const reviewEl = $el.find(selector).first();
            if (reviewEl.length) {
                const reviewText = reviewEl.text().trim();
                const reviewMatch = reviewText.match(/(\d+)/);
                if (reviewMatch) {
                    reviewCount = parseInt(reviewMatch[1], 10);
                    break;
                }
            }
        }

        const practiceAreaSelectors = [
            '[data-testid="practice-areas"]',
            '.practice-areas',
            '.specialties',
            '[class*="practice"]'
        ];

        let practiceAreas = [];
        for (const selector of practiceAreaSelectors) {
            const practiceEl = $el.find(selector);
            if (practiceEl.length) {
                practiceEl.find('li, span, a').each((_, item) => {
                    const area = $(item).text().trim();
                    if (area && area.length > 2) {
                        practiceAreas.push(area);
                    }
                });
                if (practiceAreas.length > 0) break;
            }
        }

        if (practiceAreas.length === 0) {
            for (const selector of practiceAreaSelectors) {
                const practiceEl = $el.find(selector).first();
                if (practiceEl.length) {
                    const text = practiceEl.text().trim();
                    if (text.includes(',')) {
                        practiceAreas = text.split(',').map(a => a.trim()).filter(a => a.length > 2);
                        break;
                    }
                }
            }
        }

        const locationSelectors = [
            '[data-testid="location"]',
            '.location',
            '.address',
            '[class*="location"]'
        ];

        let location = '';
        for (const selector of locationSelectors) {
            const locationEl = $el.find(selector).first();
            if (locationEl.length && locationEl.text().trim()) {
                location = locationEl.text().trim();
                break;
            }
        }

        const phoneSelectors = [
            '[data-testid="phone"]',
            '.phone',
            'a[href^="tel:"]',
            '[class*="phone"]'
        ];

        let phone = '';
        for (const selector of phoneSelectors) {
            const phoneEl = $el.find(selector).first();
            if (phoneEl.length) {
                phone = phoneEl.text().trim() || phoneEl.attr('href')?.replace('tel:', '') || '';
                if (phone) break;
            }
        }

        const websiteSelectors = [
            '[data-testid="website"]',
            'a[href*="website"]',
            '.website',
            'a[data-website]'
        ];

        let website = '';
        for (const selector of websiteSelectors) {
            const websiteEl = $el.find(selector).first();
            if (websiteEl.length) {
                website = websiteEl.attr('href') || '';
                if (website) break;
            }
        }

        const yearsLicensedSelectors = [
            '[data-testid="years-licensed"]',
            '.years-licensed',
            '[class*="years"]'
        ];

        let yearsLicensed = null;
        for (const selector of yearsLicensedSelectors) {
            const yearsEl = $el.find(selector).first();
            if (yearsEl.length) {
                const yearsText = yearsEl.text().trim();
                const yearsMatch = yearsText.match(/(\d+)/);
                if (yearsMatch) {
                    yearsLicensed = parseInt(yearsMatch[1], 10);
                    break;
                }
            }
        }

        const barSelectors = [
            '[data-testid="bar-admissions"]',
            '.bar-admissions',
            '[class*="bar"]'
        ];

        let barAdmissions = [];
        for (const selector of barSelectors) {
            const barEl = $el.find(selector);
            if (barEl.length) {
                barEl.find('li, span').each((_, item) => {
                    const bar = $(item).text().trim();
                    if (bar && bar.length > 1) {
                        barAdmissions.push(bar);
                    }
                });
                if (barAdmissions.length > 0) break;
            }
        }

        const langSelectors = [
            '[data-testid="languages"]',
            '.languages',
            '[class*="language"]'
        ];

        let languages = [];
        for (const selector of langSelectors) {
            const langEl = $el.find(selector);
            if (langEl.length) {
                langEl.find('li, span').each((_, item) => {
                    const lang = $(item).text().trim();
                    if (lang && lang.length > 1) {
                        languages.push(lang);
                    }
                });
                if (languages.length > 0) break;
            }
        }

        const bioSelectors = [
            '[data-testid="bio"]',
            '.bio',
            '.description',
            '.profile-description',
            'p'
        ];

        let bio = '';
        for (const selector of bioSelectors) {
            const bioEl = $el.find(selector).first();
            if (bioEl.length && bioEl.text().trim().length > 50) {
                bio = bioEl.text().trim();
                break;
            }
        }

        if (name || profileUrl) {
            return {
                name: name || 'Unknown',
                rating,
                reviewCount,
                practiceAreas,
                location,
                phone,
                email: '',
                website,
                yearsLicensed,
                barAdmissions,
                languages,
                profileUrl,
                bio,
                scrapedAt: new Date().toISOString()
            };
        }
        return null;
    } catch (err) {
        log.debug(`Error extracting individual lawyer: ${err.message}`);
        return null;
    }
}

/**
 * Debug: Save page HTML snippet for analysis when 0 lawyers found
 */
async function saveDebugInfo(page) {
    try {
        const html = await page.content();
        const $ = cheerio.load(html);

        const articleCount = $('article').length;
        const divLawyerCount = $('[class*="lawyer"]').length;
        const dataTestIdCount = $('[data-testid]').length;

        log.warning('DEBUG: Page structure analysis', {
            articleCount,
            divLawyerCount,
            dataTestIdCount,
            title: $('title').text(),
            hasCloudflare: html.includes('Just a moment') || html.includes('cf-browser')
        });

        await Actor.setValue('DEBUG_PAGE_HTML', html, { contentType: 'text/html' });
        log.info('Saved full page HTML to DEBUG_PAGE_HTML for analysis');

    } catch (error) {
        log.warning(`Failed to save debug info: ${error.message}`);
    }
}

/**
 * Main Actor execution
 */
try {
    const input = await Actor.getInput() || {};

    log.info('Starting Avvo Lawyers Scraper', {
        startUrl: input.startUrl,
        practiceArea: input.practiceArea,
        state: input.state,
        city: input.city,
        maxLawyers: input.maxLawyers
    });

    if (!input.startUrl?.trim() && (!input.practiceArea?.trim() || !input.state?.trim())) {
        throw new Error('Invalid input: Either provide a "startUrl" OR both "practiceArea" and "state"');
    }

    const maxLawyers = input.maxLawyers ?? 50;
    if (maxLawyers < 0 || maxLawyers > 10000) {
        throw new Error('maxLawyers must be between 0 and 10000');
    }

    const searchUrl = buildSearchUrl(input);
    log.info(`Search URL: ${searchUrl}`);

    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true }
    );

    let totalLawyersScraped = 0;
    let pagesProcessed = 0;
    let extractionMethod = 'None';
    const startTime = Date.now();

    const seenLawyerUrls = new Set();

    const proxyUrl = await proxyConfiguration.newUrl();

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: 20,
        maxConcurrency: 3,
        navigationTimeoutSecs: 30,
        requestHandlerTimeoutSecs: 120,
        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl,
                geoip: true,
                os: 'windows',
                locale: 'en-US',
                screen: {
                    minWidth: 1024,
                    maxWidth: 1920,
                    minHeight: 768,
                    maxHeight: 1080,
                },
            }),
        },

        async requestHandler({ page, request }) {
            pagesProcessed++;
            log.info(`Processing page ${pagesProcessed}: ${request.url}`);

            try {
                await page.setExtraHTTPHeaders({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                });

                await page.goto(request.url, {
                    waitUntil: 'domcontentloaded',
                    timeout: 30000
                });

                await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => { });

                let cloudflareDetected = false;
                let retryCount = 0;
                const maxRetries = 3;

                while (retryCount < maxRetries) {
                    const title = await page.title();
                    const bodyText = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');

                    if (title.includes('Just a moment') ||
                        title.includes('Cloudflare') ||
                        bodyText.includes('unusual traffic') ||
                        bodyText.includes('Checking your browser')) {

                        cloudflareDetected = true;
                        log.warning(`Cloudflare challenge detected (attempt ${retryCount + 1}/${maxRetries})`);

                        await page.waitForTimeout(3000);

                        try {
                            const turnstileFrame = page.frameLocator('iframe[src*="challenges.cloudflare.com"]');
                            const checkbox = turnstileFrame.locator('input[type="checkbox"], .cf-turnstile-wrapper');

                            if (await checkbox.count() > 0) {
                                log.info('Found Turnstile checkbox, attempting click...');
                                await checkbox.first().click({ timeout: 5000 });
                                await page.waitForTimeout(3000);
                            }
                        } catch (clickErr) {
                            log.debug('No clickable Turnstile element found');
                        }

                        await page.waitForTimeout(5000);
                        await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => { });

                        retryCount++;
                    } else {
                        if (cloudflareDetected) {
                            log.info('Cloudflare challenge bypassed successfully!');
                        }
                        break;
                    }
                }

                if (retryCount >= maxRetries) {
                    log.error('Failed to bypass Cloudflare after maximum retries');
                    await saveDebugInfo(page);
                    return;
                }

                await page.waitForTimeout(2000);

                let lawyers = [];

                // Strategy 1: Try JSON-LD extraction first
                lawyers = await extractLawyersViaJsonLD(page);
                if (lawyers.length > 0) {
                    extractionMethod = 'JSON-LD';
                    log.info(`✓ JSON-LD extraction successful: ${lawyers.length} lawyers`);
                }

                // Strategy 2: Try internal API/embedded data
                if (lawyers.length === 0) {
                    lawyers = await extractLawyersViaAPI(page);
                    if (lawyers.length > 0) {
                        extractionMethod = 'Internal API';
                        log.info(`✓ Internal API extraction successful: ${lawyers.length} lawyers`);
                    }
                }

                // Strategy 3: Fall back to HTML parsing
                if (lawyers.length === 0) {
                    lawyers = await extractLawyerDataViaHTML(page);
                    if (lawyers.length > 0) {
                        extractionMethod = 'HTML Parsing (Cheerio)';
                        log.info(`✓ HTML parsing successful: ${lawyers.length} lawyers`);
                    }
                }

                if (lawyers.length === 0) {
                    log.warning('No lawyers found with any extraction method. Saving debug info...');
                    await saveDebugInfo(page);
                }

                if (lawyers.length > 0) {
                    let lawyersToSave = maxLawyers > 0
                        ? lawyers.slice(0, Math.max(0, maxLawyers - totalLawyersScraped))
                        : lawyers;

                    const uniqueLawyers = lawyersToSave.filter(lawyer => {
                        if (!lawyer.profileUrl) return true;

                        if (seenLawyerUrls.has(lawyer.profileUrl)) {
                            log.debug(`Skipping duplicate lawyer: ${lawyer.name} (${lawyer.profileUrl})`);
                            return false;
                        }

                        seenLawyerUrls.add(lawyer.profileUrl);
                        return true;
                    });

                    if (uniqueLawyers.length < lawyersToSave.length) {
                        log.info(`Removed ${lawyersToSave.length - uniqueLawyers.length} duplicate lawyers`);
                    }

                    lawyersToSave = uniqueLawyers;

                    if (lawyersToSave.length > 0 && input.includeContactInfo) {
                        log.info('Enriching lawyers with full profiles from detail pages...');
                        lawyersToSave = await enrichLawyersWithProfiles(lawyersToSave, page);
                    }

                    if (lawyersToSave.length > 0) {
                        await Actor.pushData(lawyersToSave);
                        totalLawyersScraped += lawyersToSave.length;
                        log.info(`Saved ${lawyersToSave.length} lawyers. Total: ${totalLawyersScraped}`);
                    }

                    if (maxLawyers > 0 && totalLawyersScraped >= maxLawyers) {
                        log.info(`Reached maximum lawyers limit: ${maxLawyers}`);
                        return;
                    }

                    const hasNextPage = await page.evaluate(() => {
                        const nextButton = document.querySelector('a[rel="next"], .next-page, [class*="next"]');
                        return nextButton && !nextButton.classList.contains('disabled');
                    });

                    if (hasNextPage && totalLawyersScraped < maxLawyers) {
                        const nextPageUrl = await page.evaluate(() => {
                            const nextButton = document.querySelector('a[rel="next"], .next-page, [class*="next"]');
                            return nextButton?.href || '';
                        });

                        if (nextPageUrl && nextPageUrl.startsWith('http')) {
                            log.info(`Found next page: ${nextPageUrl}`);
                            await crawler.addRequests([{
                                url: nextPageUrl,
                                uniqueKey: nextPageUrl
                            }]);
                        }
                    } else if (!hasNextPage) {
                        log.info('No next page button found - this may be the last page');
                    }
                } else {
                    log.warning('No lawyers found on this page');
                }

            } catch (error) {
                log.error(`Error processing page: ${error.message}`, {
                    url: request.url
                });
            }
        },

        async failedRequestHandler({ request }, error) {
            log.error(`Request failed: ${request.url} - ${error.message}`);
        }
    });

    log.info('Starting crawler with Camoufox for Cloudflare bypass...');
    await crawler.run([searchUrl]);

    const endTime = Date.now();
    const duration = Math.round((endTime - startTime) / 1000);

    const statistics = {
        totalLawyersScraped,
        pagesProcessed,
        extractionMethod,
        duration: `${duration} seconds`,
        timestamp: new Date().toISOString()
    };

    await Actor.setValue('statistics', statistics);

    log.info('✓ Scraping completed successfully!', statistics);

    if (totalLawyersScraped > 0) {
        log.info(`Successfully scraped ${totalLawyersScraped} lawyers in ${duration} seconds`);
    } else {
        log.warning('No lawyers were scraped. Please check your search parameters.');
    }

} catch (error) {
    log.exception(error, 'Actor failed with error');
    throw error;
}

await Actor.exit();
