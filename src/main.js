import { Actor, log } from 'apify';
import { CheerioCrawler, RequestQueue } from 'crawlee';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

await Actor.init();

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
];

const DEFAULT_HEADERS = {
    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
};

const LABELS = {
    LISTING: 'LISTING',
    API: 'API',
    PROFILE: 'PROFILE',
    SITEMAP: 'SITEMAP',
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function randomBetween(min, max) {
    if (max <= min) return min;
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function normalizeText(value) {
    if (!value) return '';
    return String(value).replace(/\s+/g, ' ').trim();
}

function normalizeUrl(value, baseUrl) {
    if (!value) return '';
    try {
        return new URL(value, baseUrl).href;
    } catch {
        return value;
    }
}

function normalizeArray(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value.filter(Boolean);
    if (typeof value === 'string') {
        return value.split(',').map((item) => normalizeText(item)).filter(Boolean);
    }
    return [value];
}

function toNumber(value) {
    if (value === null || value === undefined) return null;
    const num = typeof value === 'number' ? value : Number(String(value).replace(/[^\d.]/g, ''));
    return Number.isFinite(num) ? num : null;
}

function toInt(value) {
    if (value === null || value === undefined) return 0;
    const num = parseInt(String(value).replace(/[^\d]/g, ''), 10);
    return Number.isFinite(num) ? num : 0;
}

function pickFirst(...values) {
    for (const value of values) {
        if (value !== null && value !== undefined && value !== '') return value;
    }
    return null;
}

function isBlockedHtml(html) {
    const snippet = html.slice(0, 5000);
    return snippet.includes('Just a moment') ||
        snippet.includes('cf-browser-verification') ||
        snippet.includes('Checking your browser') ||
        snippet.includes('Cloudflare');
}

function extractJsonLdObjects(html) {
    const $ = cheerio.load(html);
    const scripts = $('script[type="application/ld+json"]');
    const parsed = [];

    scripts.each((_, el) => {
        const text = $(el).contents().text();
        if (!text) return;
        try {
            const data = JSON.parse(text);
            parsed.push(data);
        } catch (err) {
            log.debug(`Failed to parse JSON-LD script: ${err.message}`);
        }
    });

    return parsed;
}

function extractEmbeddedJson(html) {
    const $ = cheerio.load(html);
    const extracted = [];

    const nextData = $('#__NEXT_DATA__').text();
    if (nextData) {
        try {
            extracted.push(JSON.parse(nextData));
        } catch (err) {
            log.debug(`Failed to parse __NEXT_DATA__: ${err.message}`);
        }
    }

    const jsonScripts = $('script[type="application/json"]');
    jsonScripts.each((_, el) => {
        const text = $(el).contents().text();
        if (!text || text.length < 30) return;
        try {
            extracted.push(JSON.parse(text));
        } catch {
            // Skip non-JSON blobs
        }
    });

    const inlineScripts = $('script:not([src])').toArray();
    inlineScripts.forEach((script) => {
        const content = $(script).text();
        if (!content) return;
        const apolloMatch = content.match(/__APOLLO_STATE__\s*=\s*({[\s\S]*?})\s*;?\s*$/m);
        if (apolloMatch) {
            try {
                extracted.push(JSON.parse(apolloMatch[1]));
            } catch {
                // Ignore non-JSON Apollo state
            }
        }
        const stateMatch = content.match(/__INITIAL_STATE__\s*=\s*({[\s\S]*?})\s*;?\s*$/m);
        if (stateMatch) {
            try {
                extracted.push(JSON.parse(stateMatch[1]));
            } catch {
                // Ignore non-JSON initial state
            }
        }
    });

    return extracted;
}

function collectLawyerCandidates(source, candidates = [], depth = 0) {
    if (!source || depth > 7) return candidates;
    if (Array.isArray(source)) {
        for (const item of source) {
            if (item && typeof item === 'object' && isLawyerCandidate(item)) {
                candidates.push(item);
            } else {
                collectLawyerCandidates(item, candidates, depth + 1);
            }
        }
        return candidates;
    }

    if (typeof source === 'object') {
        for (const value of Object.values(source)) {
            collectLawyerCandidates(value, candidates, depth + 1);
        }
    }
    return candidates;
}

function isLawyerCandidate(item) {
    const hasName = Boolean(item.name || item.fullName || item.displayName || item.title);
    const hasProfile = Boolean(item.profileUrl || item.profile_url || item.url || item.link);
    const hasHints = Boolean(item.practiceAreas || item.specialties || item.avvoRating || item.rating || item.location);
    return hasName && (hasProfile || hasHints);
}

function normalizeLawyer(raw, baseUrl) {
    if (!raw || typeof raw !== 'object') return null;

    const name = normalizeText(pickFirst(raw.name, raw.fullName, raw.displayName, raw.title));
    const profileUrl = normalizeUrl(pickFirst(raw.profileUrl, raw.profile_url, raw.url, raw.link), baseUrl);
    const address = raw.address || {};

    let location = '';
    if (typeof address === 'string') {
        location = normalizeText(address);
    } else if (address && typeof address === 'object') {
        location = normalizeText(
            [address.addressLocality, address.addressRegion, address.postalCode].filter(Boolean).join(', ')
        );
    } else {
        location = normalizeText(pickFirst(raw.location, raw.city, raw.region));
    }

    const practiceAreas = normalizeArray(
        pickFirst(raw.practiceAreas, raw.specialties, raw.practiceArea, raw.tags, raw.knowsAbout, raw.areaServed)
    ).map(normalizeText).filter(Boolean);

    const sameAs = normalizeArray(raw.sameAs);
    const externalSameAs = sameAs.find((item) => typeof item === 'string' && !item.includes('avvo.com'));

    return {
        name: name || 'Unknown',
        rating: toNumber(pickFirst(raw.rating, raw.avvoRating, raw.aggregateRating?.ratingValue)),
        reviewCount: toInt(pickFirst(raw.reviewCount, raw.reviews?.length, raw.aggregateRating?.reviewCount)),
        practiceAreas,
        location,
        phone: normalizeText(pickFirst(raw.phone, raw.phoneNumber, raw.telephone)),
        email: normalizeText(raw.email),
        website: normalizeUrl(pickFirst(raw.website, raw.websiteUrl, externalSameAs), baseUrl),
        yearsLicensed: toInt(pickFirst(raw.yearsLicensed, raw.yearAdmitted)),
        barAdmissions: normalizeArray(raw.barAdmissions).map(normalizeText).filter(Boolean),
        languages: normalizeArray(raw.languages).map(normalizeText).filter(Boolean),
        profileUrl,
        bio: normalizeText(pickFirst(raw.bio, raw.description)),
        education: normalizeArray(raw.education).map(normalizeText).filter(Boolean),
        awards: normalizeArray(raw.awards).map(normalizeText).filter(Boolean),
        reviews: normalizeArray(raw.reviews),
        scrapedAt: new Date().toISOString(),
    };
}

function extractLawyersFromJsonLd(html, baseUrl) {
    const jsonObjects = extractJsonLdObjects(html);
    const lawyers = [];

    for (const data of jsonObjects) {
        if (Array.isArray(data)) {
            data.forEach((item) => addJsonLdLawyer(item, lawyers, baseUrl));
        } else {
            addJsonLdLawyer(data, lawyers, baseUrl);
        }
    }

    return lawyers;
}

function addJsonLdLawyer(data, lawyers, baseUrl) {
    if (!data) return;
    if (data['@graph']) {
        data['@graph'].forEach((item) => addJsonLdLawyer(item, lawyers, baseUrl));
        return;
    }
    if (data['@type'] === 'ItemList' && data.itemListElement) {
        data.itemListElement.forEach((item) => addJsonLdLawyer(item.item || item, lawyers, baseUrl));
        return;
    }
    const type = data['@type'];
    if (type === 'Attorney' || type === 'Person' || type === 'LegalService') {
        const normalized = normalizeLawyer(data, baseUrl);
        if (normalized) lawyers.push(normalized);
    }
}

function extractApiUrlsFromHtml(html, baseUrl) {
    const candidates = new Set();
    const absoluteRegex = /https?:\/\/[^\s"'\\]+\/api\/[^\s"'\\]+/g;
    const relativeRegex = /['"]((?:\/api\/|\/graphql)[^'"\s]+)['"]/g;

    let match;
    while ((match = absoluteRegex.exec(html)) !== null) {
        candidates.add(match[0]);
    }
    while ((match = relativeRegex.exec(html)) !== null) {
        candidates.add(normalizeUrl(match[1], baseUrl));
    }

    return [...candidates];
}

function extractNextPageUrlFromHtml($, baseUrl) {
    const nextHref = $('a[rel="next"], link[rel="next"]').attr('href');
    if (nextHref) return normalizeUrl(nextHref, baseUrl);

    const nextButton = $('a[class*="next"], .pagination a').filter((_, el) => {
        const text = normalizeText($(el).text()).toLowerCase();
        return text === 'next' || text === 'next page';
    }).first();

    const href = nextButton.attr('href');
    return href ? normalizeUrl(href, baseUrl) : '';
}

function extractNextPageUrlFromApi(json, baseUrl) {
    if (!json || typeof json !== 'object') return '';
    const candidate = pickFirst(
        json.nextPageUrl,
        json.next,
        json.links?.next,
        json.pagination?.next,
        json.paging?.next
    );
    return candidate ? normalizeUrl(candidate, baseUrl) : '';
}

async function fetchJsonWithRetries(url, { proxyUrl, headers, maxRetries = 3 }) {
    let attempt = 0;
    while (attempt <= maxRetries) {
        try {
            const response = await gotScraping({
                url,
                headers,
                proxyUrl,
                timeout: { request: 20000 },
                retry: { limit: 0 },
                throwHttpErrors: false,
            });

            if ([403, 429, 503].includes(response.statusCode)) {
                throw new Error(`Blocked with status ${response.statusCode}`);
            }
            if (response.statusCode < 200 || response.statusCode >= 300) {
                throw new Error(`Unexpected status ${response.statusCode}`);
            }

            const bodyText = typeof response.body === 'string' ? response.body : response.body.toString('utf-8');
            return JSON.parse(bodyText);
        } catch (err) {
            if (attempt === maxRetries) throw err;
            const backoff = Math.min(500 * 2 ** attempt, 5000);
            await sleep(backoff + randomBetween(0, 250));
            attempt += 1;
        }
    }
    return null;
}

function extractLawyersFromApiJson(json, baseUrl) {
    const candidates = collectLawyerCandidates(json);
    const lawyers = candidates.map((item) => normalizeLawyer(item, baseUrl)).filter(Boolean);
    return lawyers;
}

function extractLawyerDataViaHtml($, baseUrl) {
    const selectors = [
        'div[data-testid="lawyer-card"]',
        '.lawyer-card',
        '[class*="lawyer"][class*="card"]',
        'article[data-lawyer-id]',
        '.search-result-lawyer',
        '.profile-card',
        '[data-lawyer-name]',
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
        return [];
    }

    const lawyers = [];
    lawyerElements.each((_, element) => {
        const lawyer = extractLawyerFromElement($, $(element), baseUrl);
        if (lawyer) lawyers.push(lawyer);
    });
    return lawyers;
}

function extractLawyerFromElement($, $el, baseUrl) {
    try {
        const nameSelectors = [
            '[data-testid="lawyer-name"]',
            'h2 a',
            'h3 a',
            '.lawyer-name',
            '.profile-name',
            'a[href*="/attorney/"]',
        ];

        let name = '';
        let profileUrl = '';

        for (const selector of nameSelectors) {
            const nameEl = $el.find(selector).first();
            if (nameEl.length && normalizeText(nameEl.text())) {
                name = normalizeText(nameEl.text());
                profileUrl = normalizeUrl(nameEl.attr('href') || '', baseUrl);
                break;
            }
        }

        const ratingSelectors = [
            '[data-testid="rating"]',
            '.rating-value',
            '.avvo-rating',
            '[class*="rating"]',
        ];

        let rating = null;
        for (const selector of ratingSelectors) {
            const ratingEl = $el.find(selector).first();
            if (ratingEl.length) {
                const ratingMatch = normalizeText(ratingEl.text()).match(/(\d+\.?\d*)/);
                if (ratingMatch) {
                    rating = parseFloat(ratingMatch[1]);
                    break;
                }
            }
        }

        const reviewSelectors = [
            '[data-testid="review-count"]',
            '.review-count',
            '[class*="review"]',
        ];

        let reviewCount = 0;
        for (const selector of reviewSelectors) {
            const reviewEl = $el.find(selector).first();
            if (reviewEl.length) {
                reviewCount = toInt(reviewEl.text());
                break;
            }
        }

        const practiceAreaSelectors = [
            '[data-testid="practice-areas"]',
            '.practice-areas',
            '.specialties',
            '[class*="practice"]',
        ];

        let practiceAreas = [];
        for (const selector of practiceAreaSelectors) {
            const practiceEl = $el.find(selector);
            if (practiceEl.length) {
                practiceEl.find('li, span, a').each((_, item) => {
                    const area = normalizeText($(item).text());
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
                    const text = normalizeText(practiceEl.text());
                    if (text.includes(',')) {
                        practiceAreas = text.split(',').map((area) => normalizeText(area)).filter(Boolean);
                        break;
                    }
                }
            }
        }

        const locationSelectors = [
            '[data-testid="location"]',
            '.location',
            '.address',
            '[class*="location"]',
        ];

        let location = '';
        for (const selector of locationSelectors) {
            const locationEl = $el.find(selector).first();
            if (locationEl.length && normalizeText(locationEl.text())) {
                location = normalizeText(locationEl.text());
                break;
            }
        }

        const phoneSelectors = [
            '[data-testid="phone"]',
            '.phone',
            'a[href^="tel:"]',
            '[class*="phone"]',
        ];

        let phone = '';
        for (const selector of phoneSelectors) {
            const phoneEl = $el.find(selector).first();
            if (phoneEl.length) {
                phone = normalizeText(phoneEl.text()) || normalizeText(phoneEl.attr('href')?.replace('tel:', ''));
                if (phone) break;
            }
        }

        const websiteSelectors = [
            '[data-testid="website"]',
            'a[href*="website"]',
            '.website',
            'a[data-website]',
        ];

        let website = '';
        for (const selector of websiteSelectors) {
            const websiteEl = $el.find(selector).first();
            if (websiteEl.length) {
                website = normalizeUrl(websiteEl.attr('href') || '', baseUrl);
                if (website) break;
            }
        }

        const yearsLicensedSelectors = [
            '[data-testid="years-licensed"]',
            '.years-licensed',
            '[class*="years"]',
        ];

        let yearsLicensed = null;
        for (const selector of yearsLicensedSelectors) {
            const yearsEl = $el.find(selector).first();
            if (yearsEl.length) {
                yearsLicensed = toInt(yearsEl.text());
                break;
            }
        }

        const barSelectors = [
            '[data-testid="bar-admissions"]',
            '.bar-admissions',
            '[class*="bar"]',
        ];

        let barAdmissions = [];
        for (const selector of barSelectors) {
            const barEl = $el.find(selector);
            if (barEl.length) {
                barEl.find('li, span').each((_, item) => {
                    const bar = normalizeText($(item).text());
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
            '[class*="language"]',
        ];

        let languages = [];
        for (const selector of langSelectors) {
            const langEl = $el.find(selector);
            if (langEl.length) {
                langEl.find('li, span').each((_, item) => {
                    const lang = normalizeText($(item).text());
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
            'p',
        ];

        let bio = '';
        for (const selector of bioSelectors) {
            const bioEl = $el.find(selector).first();
            const text = normalizeText(bioEl.text());
            if (bioEl.length && text.length > 50) {
                bio = text;
                break;
            }
        }

        if (!name && !profileUrl) return null;

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
            scrapedAt: new Date().toISOString(),
        };
    } catch (err) {
        log.debug(`Error extracting individual lawyer: ${err.message}`);
        return null;
    }
}

async function fetchLawyerProfile(profileUrl, { proxyUrl, userAgent, includeReviews }) {
    try {
        const response = await gotScraping({
            url: profileUrl,
            headers: {
                ...DEFAULT_HEADERS,
                'User-Agent': userAgent,
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
            },
            proxyUrl,
            timeout: { request: 20000 },
            retry: { limit: 0 },
            throwHttpErrors: false,
        });

        if ([403, 429, 503].includes(response.statusCode)) {
            return { blocked: true };
        }
        if (response.statusCode !== 200) {
            return null;
        }

        const html = typeof response.body === 'string' ? response.body : response.body.toString('utf-8');
        if (isBlockedHtml(html)) {
            return { blocked: true };
        }

        const $ = cheerio.load(html);
        const jsonLdProfiles = extractLawyersFromJsonLd(html, profileUrl);
        const jsonLdProfile = jsonLdProfiles[0] || null;

        const bio = normalizeText(
            $('[data-testid="bio"], .lawyer-bio, .bio-text, .profile-bio').first().text()
        );

        const education = [];
        $('[data-testid="education"] li, .education-item, .school-item, [class*="education"] li').each((_, el) => {
            const value = normalizeText($(el).text());
            if (value) education.push(value);
        });

        const awards = [];
        $('[data-testid="awards"] li, .award-item, [class*="award"] li').each((_, el) => {
            const value = normalizeText($(el).text());
            if (value) awards.push(value);
        });

        const emailLink = $('a[href^="mailto:"]').first();
        const emailHref = emailLink.attr('href') || '';
        const emailFromHtml = normalizeText(
            emailHref.replace(/^mailto:/i, '').split('?')[0] || emailLink.text()
        );

        const phoneLink = $('a[href^="tel:"]').first();
        const phoneHref = phoneLink.attr('href') || '';
        const phoneFromHtml = normalizeText(
            phoneHref.replace(/^tel:/i, '').split('?')[0] || phoneLink.text()
        ) || normalizeText($('[data-testid="phone"], .phone').first().text());

        const locationFromHtml = normalizeText(
            $('[data-testid="address"], [data-testid="location"], .profile-address, .office-address, address, .address, .location')
                .first()
                .text()
        );

        const ratingFromHtml = toNumber(
            normalizeText(
                $('[data-testid="rating"], .avvo-rating, .rating-value, [class*="rating"]')
                    .first()
                    .text()
            )
        );

        const websiteFromHtml = normalizeUrl(
            $('[data-testid="website"] a, a[data-website], a[data-event-label="Website"], a[aria-label*="Website"], a[href*="website"]')
                .first()
                .attr('href') || '',
            profileUrl
        );

        const practiceAreas = [];
        $('[data-testid="practice-areas"], .practice-areas, .specialties')
            .find('li, span, a')
            .each((_, el) => {
                const value = normalizeText($(el).text());
                if (value) practiceAreas.push(value);
            });

        let reviews = [];
        if (includeReviews) {
            const jsonLd = extractJsonLdObjects(html);
            jsonLd.forEach((item) => {
                const reviewData = normalizeArray(item.review || item.reviews);
                if (reviewData.length > 0) {
                    reviews = reviews.concat(reviewData);
                }
            });
        }

        return {
            bio,
            education,
            awards,
            reviews,
            email: pickFirst(jsonLdProfile?.email, emailFromHtml),
            phone: pickFirst(jsonLdProfile?.phone, phoneFromHtml),
            location: pickFirst(jsonLdProfile?.location, locationFromHtml),
            rating: pickFirst(jsonLdProfile?.rating, ratingFromHtml),
            reviewCount: pickFirst(jsonLdProfile?.reviewCount, null),
            website: pickFirst(jsonLdProfile?.website, websiteFromHtml),
            practiceAreas: jsonLdProfile?.practiceAreas?.length ? jsonLdProfile.practiceAreas : practiceAreas,
        };
    } catch (error) {
        log.debug(`Failed to fetch profile page ${profileUrl}: ${error.message}`);
        return null;
    }
}

async function enrichLawyersWithProfiles(lawyers, options) {
    if (lawyers.length === 0) return lawyers;

    const { maxConcurrency, proxyUrl, userAgent, includeReviews } = options;
    const enriched = [];
    let blockedCount = 0;

    for (let i = 0; i < lawyers.length; i += maxConcurrency) {
        const batch = lawyers.slice(i, i + maxConcurrency);
        const batchResults = await Promise.all(
            batch.map(async (lawyer) => {
                if (!lawyer.profileUrl) return lawyer;
                const profileData = await fetchLawyerProfile(lawyer.profileUrl, {
                    proxyUrl,
                    userAgent,
                    includeReviews,
                });
                if (profileData?.blocked) {
                    blockedCount += 1;
                    return lawyer;
                }
                if (!profileData) return lawyer;
                return {
                    ...lawyer,
                    bio: profileData.bio || lawyer.bio,
                    education: profileData.education?.length ? profileData.education : (lawyer.education || []),
                    awards: profileData.awards?.length ? profileData.awards : (lawyer.awards || []),
                    reviews: profileData.reviews?.length ? profileData.reviews : (lawyer.reviews || []),
                    email: profileData.email || lawyer.email,
                    phone: profileData.phone || lawyer.phone,
                    location: profileData.location || lawyer.location,
                    rating: profileData.rating ?? lawyer.rating,
                    reviewCount: profileData.reviewCount ?? lawyer.reviewCount,
                    website: profileData.website || lawyer.website,
                    practiceAreas: profileData.practiceAreas?.length ? profileData.practiceAreas : lawyer.practiceAreas,
                };
            })
        );
        enriched.push(...batchResults);
        if (i + maxConcurrency < lawyers.length) {
            await sleep(200);
        }
    }

    if (blockedCount > 0) {
        log.warning(`${blockedCount} profile pages were blocked - using listing data only.`);
    }
    return enriched;
}

async function saveDebugHtml({ html, key, url, extra }) {
    if (!html) return;
    await Actor.setValue(key, html, { contentType: 'text/html' });
    log.info(`Saved debug HTML for ${url} to ${key}`);
    if (extra) log.debug(extra);
}

function buildSearchUrl(input) {
    const practiceArea = input.practiceArea || 'bankruptcy-debt';
    const state = input.state || 'al';
    const city = input.city ? `${input.city.toLowerCase()}-` : '';
    return `https://www.avvo.com/${practiceArea}-lawyer/${city}${state}.html`;
}

function buildStartUrls(input) {
    if (Array.isArray(input.startUrls) && input.startUrls.length > 0) {
        return input.startUrls.map((item) => item.url).filter(Boolean);
    }
    if (input.startUrl && input.startUrl.trim()) {
        return [input.startUrl.trim()];
    }
    return [buildSearchUrl(input)];
}

async function enqueueSitemapUrls({ requestQueue, proxyUrl, limit }) {
    const sitemapCandidates = [
        'https://www.avvo.com/sitemap.xml',
        'https://www.avvo.com/sitemaps/sitemap.xml',
    ];

    const urls = new Set();
    for (const sitemapUrl of sitemapCandidates) {
        try {
            const response = await gotScraping({
                url: sitemapUrl,
                headers: DEFAULT_HEADERS,
                proxyUrl,
                timeout: { request: 20000 },
                retry: { limit: 0 },
                throwHttpErrors: false,
            });

            if (response.statusCode !== 200) continue;
            const bodyText = typeof response.body === 'string' ? response.body : response.body.toString('utf-8');

            const locMatches = bodyText.match(/<loc>([^<]+)<\/loc>/g) || [];
            locMatches.forEach((loc) => {
                const url = loc.replace('<loc>', '').replace('</loc>', '');
                if (url.includes('/attorneys/') || url.includes('/attorney/')) {
                    urls.add(url);
                }
            });
        } catch (error) {
            log.debug(`Failed to fetch sitemap ${sitemapUrl}: ${error.message}`);
        }
    }

    const limited = limit > 0 ? [...urls].slice(0, limit) : [...urls];
    for (const url of limited) {
        await requestQueue.addRequest({
            url,
            userData: { label: LABELS.PROFILE },
        });
    }

    if (limited.length > 0) {
        log.info(`Enqueued ${limited.length} profile URLs from sitemaps`);
    }
}

async function handleLawyers(lawyers, options) {
    const {
        maxLawyers,
        seenProfileUrls,
        includeContactInfo,
        includeReviews,
        proxyUrl,
        userAgent,
        maxProfileConcurrency,
        stats,
    } = options;

    let filtered = lawyers.filter((lawyer) => {
        if (!lawyer.profileUrl) return true;
        if (seenProfileUrls.has(lawyer.profileUrl)) return false;
        seenProfileUrls.add(lawyer.profileUrl);
        return true;
    });

    if (maxLawyers > 0) {
        filtered = filtered.slice(0, Math.max(0, maxLawyers - stats.totalLawyersScraped));
    }

    if (filtered.length === 0) return;

    if (includeContactInfo || includeReviews) {
        const enriched = await enrichLawyersWithProfiles(filtered, {
            maxConcurrency: maxProfileConcurrency,
            proxyUrl,
            userAgent,
            includeReviews,
        });
        stats.profileEnrichments += enriched.length;
        filtered = enriched;
    }

    await Actor.pushData(filtered);
    stats.totalLawyersScraped += filtered.length;
}

try {
    const input = await Actor.getInput() || {};

    const maxLawyers = input.maxLawyers ?? 50;
    const maxConcurrency = 10;
    const maxProfileConcurrency = 5;
    const maxRequestsPerCrawl = 1000;
    const minDelayMs = 250;
    const maxDelayMs = 1000;
    const useApiFirst = true;
    const useHtmlFallback = true;
    const useSitemaps = false;
    const apiEndpoint = '';
    const includeReviews = input.includeReviews ?? true;
    const includeContactInfo = input.includeContactInfo ?? true;

    if (!input.startUrl?.trim()
        && (!Array.isArray(input.startUrls) || input.startUrls.length === 0)
        && (!input.practiceArea?.trim() || !input.state?.trim())) {
        throw new Error('Invalid input: provide "startUrl", "startUrls", or both "practiceArea" and "state".');
    }

    if (maxLawyers < 0 || maxLawyers > 10000) {
        throw new Error('maxLawyers must be between 0 and 10000');
    }

    log.info('Starting Avvo Lawyers Scraper', {
        startUrl: input.startUrl,
        startUrls: input.startUrls?.length || 0,
        practiceArea: input.practiceArea,
        state: input.state,
        city: input.city,
        maxLawyers,
        useApiFirst,
        useHtmlFallback,
    });

    const startUrls = buildStartUrls(input);
    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true }
    );

    const stats = {
        totalLawyersScraped: 0,
        pagesProcessed: 0,
        apiExtractions: 0,
        embeddedJsonExtractions: 0,
        jsonLdExtractions: 0,
        htmlExtractions: 0,
        profileEnrichments: 0,
        blockedRequests: 0,
        timestamp: new Date().toISOString(),
    };

    const seenProfileUrls = new Set();
    const discoveredApiUrls = new Set();
    const requestQueue = await RequestQueue.open();

    if (apiEndpoint) {
        await requestQueue.addRequest({
            url: normalizeUrl(apiEndpoint, 'https://www.avvo.com'),
            userData: { label: LABELS.API },
            forefront: true,
        });
    }

    for (const url of startUrls) {
        await requestQueue.addRequest({
            url,
            userData: { label: LABELS.LISTING },
        });
    }

    if (useSitemaps) {
        const proxyUrl = await proxyConfiguration.newUrl();
        await enqueueSitemapUrls({
            requestQueue,
            proxyUrl,
            limit: maxLawyers > 0 ? maxLawyers : 0,
        });
    }

    const crawler = new CheerioCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency,
        maxRequestsPerCrawl,
        requestHandlerTimeoutSecs: 120,
        useSessionPool: true,
        sessionPoolOptions: {
            sessionOptions: {
                maxErrorScore: 3,
            },
        },
        preNavigationHooks: [
            ({ session }, gotOptions) => {
                if (!session.userData.userAgent) {
                    session.userData.userAgent = USER_AGENTS[randomBetween(0, USER_AGENTS.length - 1)];
                }
                gotOptions.headers = {
                    ...DEFAULT_HEADERS,
                    ...gotOptions.headers,
                    'User-Agent': session.userData.userAgent,
                };
            },
        ],
        async requestHandler(context) {
            const { request, body, $: cheerioRoot, session, proxyInfo } = context;
            const baseUrl = request.loadedUrl || request.url;
            stats.pagesProcessed += 1;

            const rawHtml = typeof body === 'string' ? body : body?.toString('utf-8') || '';
            if (rawHtml && isBlockedHtml(rawHtml)) {
                stats.blockedRequests += 1;
                session.markBad();
                if (input.debugHtml) {
                    await saveDebugHtml({
                        html: rawHtml,
                        key: `DEBUG_BLOCKED_${stats.pagesProcessed}`,
                        url: request.url,
                    });
                }
                throw new Error('Blocked by anti-bot protection');
            }

            if (request.userData.label === LABELS.API) {
                try {
                    const json = await fetchJsonWithRetries(request.url, {
                        proxyUrl: proxyInfo?.url,
                        headers: {
                            ...DEFAULT_HEADERS,
                            'User-Agent': session.userData.userAgent,
                        },
                    });
                    const lawyers = extractLawyersFromApiJson(json, baseUrl);
                    if (lawyers.length > 0) {
                        stats.apiExtractions += lawyers.length;
                        await handleLawyers(lawyers, {
                            maxLawyers,
                            seenProfileUrls,
                            includeContactInfo: includeContactInfo,
                            includeReviews: includeReviews,
                            proxyUrl: proxyInfo?.url,
                            userAgent: session.userData.userAgent,
                            maxProfileConcurrency,
                            stats,
                        });
                    }
                    const nextPageUrl = extractNextPageUrlFromApi(json, baseUrl);
                    if (nextPageUrl) {
                        await requestQueue.addRequest({
                            url: nextPageUrl,
                            userData: { label: LABELS.API },
                        });
                    }
                } catch (error) {
                    log.debug(`API request failed (${request.url}): ${error.message}`);
                }
            } else if (request.userData.label === LABELS.PROFILE) {
                const lawyer = {
                    name: '',
                    profileUrl: request.url,
                    scrapedAt: new Date().toISOString(),
                };
                let profile = lawyer;
                if (includeContactInfo || includeReviews) {
                    const enriched = await fetchLawyerProfile(request.url, {
                        proxyUrl: proxyInfo?.url,
                        userAgent: session.userData.userAgent,
                        includeReviews: includeReviews,
                    });
                    if (enriched && !enriched.blocked) {
                        profile = {
                            ...lawyer,
                            bio: enriched.bio,
                            education: enriched.education,
                            awards: enriched.awards,
                            reviews: enriched.reviews,
                        };
                    }
                }
                await Actor.pushData(profile);
                stats.totalLawyersScraped += 1;
            } else {
                const lawyers = [];

                if (useApiFirst) {
                    const apiUrls = extractApiUrlsFromHtml(rawHtml, baseUrl)
                        .filter((url) => !discoveredApiUrls.has(url));
                    for (const url of apiUrls) {
                        discoveredApiUrls.add(url);
                        await requestQueue.addRequest({
                            url,
                            userData: { label: LABELS.API },
                            forefront: true,
                        });
                    }
                }

                const embeddedJson = extractEmbeddedJson(rawHtml);
                if (embeddedJson.length > 0) {
                    const embeddedLawyers = [];
                    embeddedJson.forEach((payload) => {
                        embeddedLawyers.push(...extractLawyersFromApiJson(payload, baseUrl));
                    });
                    if (embeddedLawyers.length > 0) {
                        lawyers.push(...embeddedLawyers);
                        stats.embeddedJsonExtractions += embeddedLawyers.length;
                    }
                }

                if (lawyers.length === 0) {
                    const jsonLdLawyers = extractLawyersFromJsonLd(rawHtml, baseUrl);
                    if (jsonLdLawyers.length > 0) {
                        lawyers.push(...jsonLdLawyers);
                        stats.jsonLdExtractions += jsonLdLawyers.length;
                    }
                }

                if (lawyers.length === 0 && useHtmlFallback) {
                    if (cheerioRoot) {
                        const htmlLawyers = extractLawyerDataViaHtml(cheerioRoot, baseUrl);
                        if (htmlLawyers.length > 0) {
                            lawyers.push(...htmlLawyers);
                            stats.htmlExtractions += htmlLawyers.length;
                        }
                    }
                }

                if (lawyers.length === 0 && input.debugHtml) {
                    await saveDebugHtml({
                        html: rawHtml,
                        key: `DEBUG_NO_RESULTS_${stats.pagesProcessed}`,
                        url: request.url,
                    });
                }

                if (lawyers.length > 0) {
                    await handleLawyers(lawyers, {
                        maxLawyers,
                        seenProfileUrls,
                        includeContactInfo: includeContactInfo,
                        includeReviews: includeReviews,
                        proxyUrl: proxyInfo?.url,
                        userAgent: session.userData.userAgent,
                        maxProfileConcurrency,
                        stats,
                    });
                }

                if (maxLawyers > 0 && stats.totalLawyersScraped >= maxLawyers) {
                    return;
                }

                if (cheerioRoot) {
                    const nextPageUrl = extractNextPageUrlFromHtml(cheerioRoot, baseUrl);
                    if (nextPageUrl) {
                        await requestQueue.addRequest({
                            url: nextPageUrl,
                            userData: { label: LABELS.LISTING },
                        });
                    }
                }
            }

            if (maxDelayMs > 0) {
                await sleep(randomBetween(minDelayMs, maxDelayMs));
            }
        },
        failedRequestHandler: async ({ request }, error) => {
            log.warning(`Request failed: ${request.url} - ${error.message}`);
        },
    });

    await crawler.run();

    await Actor.setValue('statistics', {
        ...stats,
        finishedAt: new Date().toISOString(),
    });

    if (stats.totalLawyersScraped > 0) {
        log.info(`Scraping completed: ${stats.totalLawyersScraped} lawyers saved`);
    } else {
        log.warning('No lawyers were scraped. Check input parameters or enable browser fallback.');
    }
} catch (error) {
    log.exception(error, 'Actor failed with error');
    throw error;
} finally {
    await Actor.exit();
}

