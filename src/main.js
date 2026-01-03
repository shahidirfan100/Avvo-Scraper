import { Actor, log } from 'apify';
import { CheerioCrawler, RequestQueue } from 'crawlee';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

await Actor.init();

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
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

function normalizeObjectText(value) {
    if (!value || typeof value !== 'object') return '';
    return normalizeText(value.value || value.text || '');
}

function normalizeUrl(value, baseUrl) {
    if (!value) return '';
    try {
        return new URL(value, baseUrl).href;
    } catch {
        return value;
    }
}

function normalizeExternalWebsite(value, baseUrl) {
    const normalized = normalizeUrl(value, baseUrl);
    if (!normalized) return '';
    try {
        const host = new URL(normalized).hostname.toLowerCase();
        if (host.includes('avvo.com')) return '';
    } catch {
        return normalized;
    }
    return normalized;
}

function normalizeImage(value, baseUrl) {
    if (!value) return '';

    let imgUrl = '';
    if (Array.isArray(value)) {
        imgUrl = normalizeImage(value[0], baseUrl);
    } else if (typeof value === 'string') {
        imgUrl = normalizeUrl(value, baseUrl);
    } else if (typeof value === 'object') {
        imgUrl = normalizeUrl(
            value.url || value.contentUrl || value['@id'] || value.thumbnailUrl || '',
            baseUrl
        );
    }

    // Filter out placeholder images
    if (imgUrl) {
        const urlLower = imgUrl.toLowerCase();
        if (urlLower.includes('placeholder') ||
            urlLower.includes('default-avatar') ||
            urlLower.includes('default_avatar') ||
            urlLower.includes('no-photo') ||
            urlLower.includes('no_photo') ||
            urlLower.includes('ghost.svg') ||  // Avvo placeholder image
            urlLower.includes('ghost-')) {     // Other ghost placeholders
            return '';
        }
    }

    return imgUrl;
}

function pickAttrValue($el, attrs) {
    if (!$el || !$el.length) return '';
    for (const attr of attrs) {
        const value = $el.attr(attr);
        if (value) return value;
    }
    return '';
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

function extractLicenseYear($, html) {
    // Look for "Licensed for X years" text in HTML
    const licenseMatch = html.match(/Licensed for (\d+) years/i);
    if (licenseMatch) {
        const yearsLicensed = parseInt(licenseMatch[1], 10);
        const currentYear = new Date().getFullYear();
        return currentYear - yearsLicensed;
    }

    // Fallback to HTML selectors
    if ($) {
        const licenseYearEl = $('[data-testid="license-year"], .license-year, [class*="license-year"]').first();
        const yearText = normalizeText(licenseYearEl.text());
        const yearMatch = yearText.match(/(\d{4})/);
        if (yearMatch) {
            return parseInt(yearMatch[1], 10);
        }

        // Try to extract from "First Licensed" text
        const firstLicensedEl = $('[class*="first-licensed"], [class*="admitted"]').first();
        const firstLicensedText = normalizeText(firstLicensedEl.text());
        const firstLicensedMatch = firstLicensedText.match(/(\d{4})/);
        if (firstLicensedMatch) {
            return parseInt(firstLicensedMatch[1], 10);
        }
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

function pickBestProfile(candidates, profileUrl) {
    if (!candidates || candidates.length === 0) return null;
    const normalizedProfileUrl = normalizeUrl(profileUrl, profileUrl);
    let best = candidates[0];
    let bestScore = -1;

    for (const candidate of candidates) {
        let score = 0;
        if (candidate.profileUrl && normalizeUrl(candidate.profileUrl, profileUrl) === normalizedProfileUrl) {
            score += 5;
        }
        if (candidate.email) score += 2;
        if (candidate.phone) score += 2;
        if (candidate.location) score += 2;
        if (candidate.rating) score += 2;
        if (candidate.website) score += 1;
        if (candidate.image) score += 1;
        if (candidate.bio) score += 1;
        if (candidate.practiceAreas && candidate.practiceAreas.length > 0) score += 1;
        if (score > bestScore) {
            bestScore = score;
            best = candidate;
        }
    }

    return best;
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
        const locationParts = [
            raw.location,
            raw.city,
            raw.state,
            raw.region,
            raw.postalCode,
            raw.zip,
        ].filter(Boolean);
        location = normalizeText(locationParts.join(', '));
    }

    const practiceAreas = normalizeArray(
        pickFirst(
            raw.practiceAreas,
            raw.practice_areas,
            raw.specialties,
            raw.practiceArea,
            raw.tags,
            raw.knowsAbout,
            raw.areaServed
        )
    ).map(normalizeText).filter(Boolean);

    const contactPoints = normalizeArray(raw.contactPoint).filter((item) => item && typeof item === 'object');
    const contactEmail = contactPoints.map((item) => item.email).find(Boolean);
    const contactPhone = contactPoints.map((item) => item.telephone || item.phone).find(Boolean);

    // Enhanced image extraction with better prioritization
    const image = normalizeImage(
        pickFirst(raw.image, raw.photo, raw.logo, raw.profilePhoto, raw.avatar, raw.photoUrl, raw.imageUrl),
        baseUrl
    );

    const contactInfo = raw.contactInfo || raw.contact || {};
    const contactWebsite = contactInfo.website || contactInfo.url || contactInfo.site;

    const sameAs = normalizeArray(raw.sameAs);
    const externalSameAs = sameAs.find((item) => typeof item === 'string' && !item.includes('avvo.com'));

    // Extract coordinates from geo field
    const geo = raw.geo || {};
    const latitude = toNumber(geo.latitude || geo.lat);
    const longitude = toNumber(geo.longitude || geo.lng || geo.lon);

    // Extract education/alumni data
    const alumniOf = normalizeArray(raw.alumniOf || raw.alumni);
    const educationList = normalizeArray(raw.education);
    const combinedEducation = [...alumniOf, ...educationList]
        .map((item) => {
            if (typeof item === 'string') return normalizeText(item);
            if (item && typeof item === 'object') {
                const schoolName = normalizeText(item.name || item.school || item.institution);
                return schoolName || normalizeText(item.description);
            }
            return null;
        })
        .filter(Boolean);

    // Extract awards
    const awardsList = normalizeArray(pickFirst(raw.award, raw.awards, raw.honors));
    const processedAwards = awardsList
        .map((item) => {
            if (typeof item === 'string') return normalizeText(item);
            if (item && typeof item === 'object') {
                return normalizeText(item.name || item.title || item.description);
            }
            return null;
        })
        .filter(Boolean);

    // Extract license states/jurisdictions
    const memberOf = normalizeArray(raw.memberOf || raw.jurisdiction);
    const licenseStates = memberOf
        .map((item) => {
            if (typeof item === 'string') return normalizeText(item);
            if (item && typeof item === 'object') {
                return normalizeText(item.name || item.state || item.jurisdiction);
            }
            return null;
        })
        .filter(Boolean);

    // Extract certifications
    const credentials = normalizeArray(raw.hasCredential || raw.credentials || raw.certifications);
    const certificationsList = credentials
        .map((item) => {
            if (typeof item === 'string') return normalizeText(item);
            if (item && typeof item === 'object') {
                return normalizeText(item.name || item.title || item.credential);
            }
            return null;
        })
        .filter(Boolean);

    // Separate Avvo rating (0-10 scale) from client rating (aggregateRating)
    const avvoRating = toNumber(pickFirst(raw.avvoRating, raw.avvo_rating));
    const clientRating = toNumber(raw.aggregateRating?.ratingValue);

    // Use avvoRating if available, otherwise fall back to aggregateRating or generic rating
    const primaryRating = avvoRating || clientRating || toNumber(pickFirst(raw.rating, raw.ratingValue));

    return {
        name: name || 'Unknown',
        rating: primaryRating,
        avvoRating: avvoRating,
        clientRating: clientRating,
        reviewCount: toInt(
            pickFirst(
                raw.reviewCount,
                raw.review_count,
                raw.reviews?.length,
                raw.aggregateRating?.reviewCount,
                raw.aggregateRating?.ratingCount
            )
        ),
        practiceAreas,
        location,
        phone: normalizeText(
            pickFirst(raw.phone, raw.phoneNumber, raw.telephone, contactPhone, contactInfo.phone)
        ),
        email: normalizeText(pickFirst(raw.email, contactEmail, contactInfo.email)),
        website: normalizeExternalWebsite(
            pickFirst(raw.website, raw.websiteUrl, contactWebsite, externalSameAs),
            baseUrl
        ),
        licenseYear: null, // Will be set from HTML parsing
        licenseStates,
        barAdmissions: normalizeArray(raw.barAdmissions).map(normalizeText).filter(Boolean),
        languages: normalizeArray(pickFirst(raw.languages, raw.language)).map(normalizeText).filter(Boolean),
        profileUrl,
        bio: normalizeText(pickFirst(raw.bio, raw.biography, raw.summary, raw.about, raw.description)),
        education: combinedEducation,
        awards: processedAwards,
        certifications: certificationsList,
        reviews: normalizeArray(raw.reviews),
        image,
        coordinates: latitude && longitude ? { latitude, longitude } : null,
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
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            const response = await gotScraping({
                url,
                headers,
                proxyUrl,
                timeout: { request: 30000 }, // Increased from 20s
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
            if (attempt === maxRetries) {
                log.warning(`Failed after ${maxRetries + 1} attempts: ${url} - ${err.message}`);
                throw err;
            }
            const backoff = Math.min(1000 * 2 ** attempt, 10000); // 1s, 2s, 4s, 8s, 10s
            log.debug(`Retry ${attempt + 1}/${maxRetries} after ${backoff}ms for ${url}`);
            await sleep(backoff + randomBetween(0, 500));
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
            '.profile-summary',
            '.lawyer-bio',
            '.bio-text',
            '[itemprop="description"]',
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

        const imageEl = $el.find('img').first();
        const headshotImgEl = $el.find('div.headshot img, .headshot img').first();
        const image = normalizeImage(
            pickAttrValue(headshotImgEl.length > 0 ? headshotImgEl : imageEl, ['src', 'data-src']) ||
            headshotImgEl.attr('src') ||
            imageEl.attr('src') ||
            imageEl.attr('data-src') || '',
            baseUrl
        );

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
            barAdmissions,
            languages,
            profileUrl,
            bio,
            image,
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
        const embeddedPayloads = extractEmbeddedJson(html);
        const embeddedCandidates = [];
        embeddedPayloads.forEach((payload) => {
            embeddedCandidates.push(...extractLawyersFromApiJson(payload, profileUrl));
        });
        const embeddedProfile = pickBestProfile(embeddedCandidates, profileUrl);

        const jsonLdProfiles = extractLawyersFromJsonLd(html, profileUrl);
        const jsonLdProfile = pickBestProfile(jsonLdProfiles, profileUrl);

        const metaDescription = normalizeText(
            $('meta[name="description"], meta[property="og:description"]').first().attr('content')
        );
        const bioFromHtml = normalizeText(
            $('[data-testid="bio"], .lawyer-bio, .bio-text, .profile-bio, [itemprop="description"]').first().text()
        );
        const bio = pickFirst(bioFromHtml, metaDescription, jsonLdProfile?.bio, embeddedProfile?.bio) || '';

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
        const emailFromData = normalizeText(
            pickAttrValue(
                $('[data-email], [data-contact-email], [data-testid="email"], .email, .contact-email').first(),
                ['data-email', 'data-contact-email']
            ) ||
            $('[data-email], [data-contact-email], [data-testid="email"], .email, .contact-email')
                .first()
                .text()
        );

        const phoneLink = $('a[href^="tel:"]').first();
        const phoneHref = phoneLink.attr('href') || '';
        const phoneFromHtml = normalizeText(
            phoneHref.replace(/^tel:/i, '').split('?')[0] || phoneLink.text()
        ) || normalizeText($('[data-testid="phone"], .phone, .contact-phone').first().text());
        const phoneFromData = normalizeText(
            pickAttrValue(
                $('[data-phone], [data-contact-phone], [data-testid="phone"]').first(),
                ['data-phone', 'data-contact-phone']
            ) ||
            $('[data-phone], [data-contact-phone], [data-testid="phone"]').first().text()
        );

        const locationFromHtml = normalizeText(
            $('[data-testid="address"], [data-testid="location"], .profile-address, .office-address, address, .address, .location')
                .first()
                .text()
        );

        const ratingFromMeta = toNumber(
            $('meta[itemprop="ratingValue"], meta[property="ratingValue"], meta[name="rating"]')
                .first()
                .attr('content')
        );

        // Use specific CSS selector for rating from detail page
        const reviewScoreLink = $('a.review-score').first();
        const ratingFromReviewScore = toNumber(normalizeText(reviewScoreLink.text()));

        // Also try the Avvo rating badge (e.g., "Rating: 10.0")
        const avvoRatingBadge = $('span.avvo-rating-count').first();
        let avvoRatingFromBadge = null;
        if (avvoRatingBadge.length > 0) {
            const badgeText = normalizeText(avvoRatingBadge.text());
            const ratingMatch = badgeText.match(/(\d+\.?\d*)/);
            if (ratingMatch) {
                avvoRatingFromBadge = toNumber(ratingMatch[1]);
            }
        }

        const ratingFromHtml = ratingFromReviewScore || toNumber(
            normalizeText(
                $('[data-testid="rating"], .avvo-rating, .rating-value, [class*="rating"]')
                    .first()
                    .text()
            )
        );

        const reviewCountFromMeta = toInt(
            $('meta[itemprop="reviewCount"], meta[itemprop="ratingCount"], meta[name="reviewCount"]')
                .first()
                .attr('content')
        );

        // Use specific CSS selector for review count from detail page
        const reviewCountSpan = $('span.review-count').first();
        const reviewCountFromSpan = toInt(normalizeText(reviewCountSpan.text()));
        const reviewCountFromHtml = reviewCountFromSpan || toInt(
            normalizeText(
                $('[data-testid="review-count"], [class*="review-count"], [itemprop="reviewCount"]')
                    .first()
                    .text()
            )
        );

        const websiteFromHtml = normalizeExternalWebsite(
            pickAttrValue(
                $('[data-testid="website"] a, a[data-website], a[data-event-label="Website"], a[aria-label*="Website"], a[href*="website"], [data-website-url], [data-url]')
                    .first(),
                ['href', 'data-website-url', 'data-url']
            ),
            profileUrl
        );

        // Use normalizeImage (not normalizeUrl) to filter out Avvo logos
        const imageFromMeta = normalizeImage(
            $('meta[property="og:image"], meta[name="twitter:image"], meta[itemprop="image"]').first().attr('content') || '',
            profileUrl
        );

        // Use specific CSS selector for profile image from detail page
        const headshotImg = $('div.headshot img').first();
        const imageFromHtml = normalizeImage(
            pickAttrValue(headshotImg, ['src', 'data-src', 'data-lazy-src']) ||
            pickAttrValue(
                $('[data-testid="profile-photo"] img, .profile-photo img, .profile-header img, img[alt*="Attorney"], img[alt*="Lawyer"], img[itemprop="image"]')
                    .first(),
                ['src', 'data-src', 'data-lazy-src']
            ),
            profileUrl
        );

        // Use specific CSS selector for practice areas from detail page
        // span.practice-area-list contains comma-separated text like "Divorce & Separation, Family, Child Custody"
        const practiceAreas = [];
        const practiceAreaList = $('span.practice-area-list');
        if (practiceAreaList.length > 0) {
            const practiceText = normalizeText(practiceAreaList.text());
            if (practiceText) {
                // Split by comma and add each practice area
                practiceText.split(',').forEach((area) => {
                    const trimmed = normalizeText(area);
                    if (trimmed && trimmed.length > 1) {
                        practiceAreas.push(trimmed);
                    }
                });
            }
        }

        // Fallback: Try detailed practice area section
        if (practiceAreas.length === 0) {
            $('div.practice-area-detail strong').each((_, el) => {
                const value = normalizeText($(el).text());
                if (value && value.length > 1) practiceAreas.push(value);
            });
        }

        // Fallback: Try generic selectors
        if (practiceAreas.length === 0) {
            $('[data-testid="practice-areas"], .practice-areas, .specialties')
                .find('li, span, a')
                .each((_, el) => {
                    const value = normalizeText($(el).text());
                    if (value && value.length > 1) practiceAreas.push(value);
                });
        }

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
            email: pickFirst(jsonLdProfile?.email, embeddedProfile?.email, emailFromData, emailFromHtml),
            phone: pickFirst(jsonLdProfile?.phone, embeddedProfile?.phone, phoneFromData, phoneFromHtml),
            location: pickFirst(jsonLdProfile?.location, embeddedProfile?.location, locationFromHtml),
            rating: pickFirst(jsonLdProfile?.rating, embeddedProfile?.rating, ratingFromMeta, ratingFromHtml),
            avvoRating: pickFirst(avvoRatingFromBadge, jsonLdProfile?.avvoRating, embeddedProfile?.avvoRating),
            clientRating: pickFirst(ratingFromReviewScore, jsonLdProfile?.clientRating, embeddedProfile?.clientRating),
            reviewCount: pickFirst(
                reviewCountFromHtml,
                jsonLdProfile?.reviewCount,
                embeddedProfile?.reviewCount,
                reviewCountFromMeta
            ),
            website: pickFirst(jsonLdProfile?.website, embeddedProfile?.website, websiteFromHtml),
            image: pickFirst(imageFromHtml, imageFromMeta, jsonLdProfile?.image, embeddedProfile?.image),
            practiceAreas: practiceAreas.length > 0
                ? practiceAreas
                : (jsonLdProfile?.practiceAreas?.length ? jsonLdProfile.practiceAreas : (embeddedProfile?.practiceAreas || [])),
            licenseYear: extractLicenseYear($, html),
            coordinates: pickFirst(jsonLdProfile?.coordinates, embeddedProfile?.coordinates),
            licenseStates: pickFirst(jsonLdProfile?.licenseStates, embeddedProfile?.licenseStates),
            certifications: pickFirst(jsonLdProfile?.certifications, embeddedProfile?.certifications),
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
                    avvoRating: profileData.avvoRating ?? lawyer.avvoRating,
                    clientRating: profileData.clientRating ?? lawyer.clientRating,
                    reviewCount: profileData.reviewCount ?? lawyer.reviewCount,
                    website: profileData.website || lawyer.website,
                    practiceAreas: profileData.practiceAreas?.length ? profileData.practiceAreas : lawyer.practiceAreas,
                    image: profileData.image || lawyer.image,
                    licenseYear: profileData.licenseYear ?? lawyer.licenseYear,
                    coordinates: profileData.coordinates || lawyer.coordinates,
                    licenseStates: profileData.licenseStates?.length ? profileData.licenseStates : (lawyer.licenseStates || []),
                    certifications: profileData.certifications?.length ? profileData.certifications : (lawyer.certifications || []),
                };
            })
        );
        enriched.push(...batchResults);
        if (i + maxConcurrency < lawyers.length) {
            await sleep(100); // Reduced from 200ms for faster enrichment
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
    // Deprecated - kept for backwards compatibility but no longer used
    return input.startUrl || 'https://www.avvo.com/bankruptcy-debt-lawyer/al.html';
}

function buildStartUrls(input) {
    if (Array.isArray(input.startUrls) && input.startUrls.length > 0) {
        return input.startUrls.map((item) => item.url || item).filter(Boolean);
    }
    if (input.startUrl && input.startUrl.trim()) {
        return [input.startUrl.trim()];
    }
    // Default fallback
    return ['https://www.avvo.com/bankruptcy-debt-lawyer/al.html'];
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

    // Process and push data in batches of 10 for incremental saving
    const BATCH_SIZE = 10;
    for (let i = 0; i < filtered.length; i += BATCH_SIZE) {
        const batch = filtered.slice(i, i + BATCH_SIZE);

        let processedBatch = batch;
        if (includeContactInfo || includeReviews) {
            processedBatch = await enrichLawyersWithProfiles(batch, {
                maxConcurrency: maxProfileConcurrency,
                proxyUrl,
                userAgent,
                includeReviews,
            });
            stats.profileEnrichments += processedBatch.length;
        }

        // Push batch immediately
        await Actor.pushData(processedBatch);
        stats.totalLawyersScraped += processedBatch.length;

        log.info(`Pushed batch of ${processedBatch.length} lawyers (total: ${stats.totalLawyersScraped})`);
    }
}

try {
    const input = await Actor.getInput() || {};

    const maxLawyers = input.maxLawyers ?? 50;
    const maxConcurrency = 20; // Increased from 10 for better speed
    const maxProfileConcurrency = 5;
    const maxRequestsPerCrawl = 1000;
    const minDelayMs = 500; // Increased for better stealth
    const maxDelayMs = 2000; // Increased for more human-like behavior
    const useApiFirst = true;
    const useHtmlFallback = true;
    const includeReviews = true; // Always include reviews
    const includeContactInfo = input.includeContactInfo ?? true;

    // Validate startUrl is provided
    if (!input.startUrl?.trim() && (!Array.isArray(input.startUrls) || input.startUrls.length === 0)) {
        throw new Error('Invalid input: "startUrl" is required.');
    }

    if (maxLawyers < 0 || maxLawyers > 10000) {
        throw new Error('maxLawyers must be between 0 and 10000');
    }

    log.info('Starting Avvo Lawyers Scraper', {
        startUrl: input.startUrl,
        startUrls: input.startUrls?.length || 0,
        maxLawyers,
        includeContactInfo,
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

    for (const url of startUrls) {
        await requestQueue.addRequest({
            url,
            userData: { label: LABELS.LISTING },
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
            ({ session, request }, gotOptions) => {
                // Set User-Agent once per session (more realistic)
                if (!session.userData.userAgent) {
                    session.userData.userAgent = USER_AGENTS[randomBetween(0, USER_AGENTS.length - 1)];
                }
                gotOptions.headers = {
                    ...DEFAULT_HEADERS,
                    ...gotOptions.headers,
                    'User-Agent': session.userData.userAgent,
                    'Referer': session.userData.lastUrl || 'https://www.avvo.com/',
                    'Accept-Language': 'en-US,en;q=0.9',
                };
                // Save current URL as referer for next request
                session.userData.lastUrl = request.url;
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
                    log.info(`Reached maxLawyers limit (${maxLawyers}). Stopping processing.`);
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

