# Avvo Lawyers Scraper

Extract comprehensive attorney profiles from Avvo with detailed practice areas, ratings, reviews, contact information, and professional credentials.

---

## Overview

The **Avvo Lawyers Scraper** extracts structured data from Avvo.com, the leading online legal directory platform. Access detailed attorney profiles including practice areas, client reviews, ratings, contact information, bar admissions, education, and professional experience.

### Key Benefits

- **Legal Research** - Build comprehensive attorney databases for research and analysis
- **Lead Generation** - Find qualified attorneys by practice area and location
- **Market Intelligence** - Analyze legal services market trends and competition
- **Directory Services** - Power legal directory platforms with fresh, accurate data
- **Competitive Analysis** - Monitor attorney rankings and review trends
- **Client Matching** - Connect clients with attorneys based on specialization

---

## Features

### Data Extraction

Extract complete attorney profiles with:

- **Personal Information** - Attorney name and profile URL
- **Professional Ratings** - Avvo rating (1-10 scale) and review count
- **Practice Areas** - Specializations and areas of legal expertise
- **Contact Details** - Phone numbers, email addresses, office locations
- **Credentials** - Years licensed, bar admissions, education
- **Languages** - Languages spoken for client communication
- **Professional Bio** - Attorney background and experience
- **Awards & Recognition** - Professional achievements and honors

### Search Options

Filter attorney searches by:

- **Practice Area** - Bankruptcy, family law, criminal defense, personal injury, real estate, business law, immigration, employment, estate planning, traffic tickets, and more
- **Location** - State-level or city-level targeting
- **Custom URLs** - Direct Avvo directory URLs for precise searches

### Performance

- **Multiple Extraction Methods** - JSON-LD schema, internal APIs, and HTML parsing with automatic fallback
- **Cloudflare Bypass** - Built-in evasion using Camoufox browser technology
- **Concurrent Processing** - Fast parallel scraping with configurable concurrency
- **Smart Deduplication** - Automatic duplicate removal across pages
- **Profile Enrichment** - Optional deep profile extraction from detail pages

---

## Quick Start

### Input Configuration

```json
{
  "startUrl": "https://www.avvo.com/bankruptcy-debt-lawyer/al.html",
  "practiceArea": "bankruptcy-debt",
  "state": "al",
  "city": "birmingham",
  "maxLawyers": 50,
  "includeReviews": true,
  "includeContactInfo": true,
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

### Run on Apify Platform

1. Open the Actor in Apify Console
2. Configure your search parameters (practice area, state, optional city)
3. Set maximum lawyers to scrape
4. Enable profile enrichment options if needed
5. Click "Start" and wait for results
6. Download data in JSON, CSV, Excel, or other formats

---

## Input Parameters

<table>
<thead>
<tr>
<th>Parameter</th>
<th>Type</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>startUrl</code></td>
<td>String</td>
<td>❌ No</td>
<td>Direct Avvo directory URL (e.g., https://www.avvo.com/bankruptcy-debt-lawyer/al.html). If provided, practiceArea and state are ignored.</td>
</tr>

<tr>
<td><code>practiceArea</code></td>
<td>String</td>
<td>⚠️ Conditional</td>
<td>Practice area slug (e.g., bankruptcy-debt, family, criminal-defense, personal-injury). Required if startUrl not provided.</td>
</tr>

<tr>
<td><code>state</code></td>
<td>String</td>
<td>⚠️ Conditional</td>
<td>Two-letter US state code (e.g., al, ca, ny, tx, fl). Required if startUrl not provided.</td>
</tr>

<tr>
<td><code>city</code></td>
<td>String</td>
<td>❌ No</td>
<td>Optional city name to narrow search within state (e.g., birmingham, montgomery)</td>
</tr>

<tr>
<td><code>maxLawyers</code></td>
<td>Integer</td>
<td>❌ No</td>
<td>Maximum number of profiles to scrape (default: 50, 0 = unlimited)</td>
</tr>

<tr>
<td><code>includeReviews</code></td>
<td>Boolean</td>
<td>❌ No</td>
<td>Extract client reviews and detailed ratings (default: true)</td>
</tr>

<tr>
<td><code>includeContactInfo</code></td>
<td>Boolean</td>
<td>❌ No</td>
<td>Fetch additional contact details from profile pages (default: true)</td>
</tr>

<tr>
<td><code>proxyConfiguration</code></td>
<td>Object</td>
<td>❌ No</td>
<td>Proxy settings for bypassing protection (recommended: useApifyProxy: true)</td>
</tr>

</tbody>
</table>

---

## Output Data

Each lawyer profile includes:

```json
{
  "name": "John Smith",
  "rating": 9.5,
  "reviewCount": 47,
  "practiceAreas": ["Bankruptcy", "Debt Collection Defense", "Foreclosure Defense"],
  "location": "Birmingham, AL 35203",
  "phone": "(205) 555-0123",
  "email": "contact@example.com",
  "website": "https://www.example.com",
  "yearsLicensed": 15,
  "barAdmissions": ["Alabama State Bar", "U.S. District Court"],
  "languages": ["English", "Spanish"],
  "profileUrl": "https://www.avvo.com/attorneys/12345-john-smith.html",
  "bio": "Experienced bankruptcy attorney serving Birmingham and surrounding areas...",
  "education": ["Harvard Law School, J.D., 2008", "Yale University, B.A., 2005"],
  "awards": ["Super Lawyers Rising Star 2022", "Avvo Client's Choice 2023"],
  "scrapedAt": "2026-01-02T10:30:00.000Z"
}
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `name` | String | Attorney full name |
| `rating` | Number | Avvo rating (1-10 scale) |
| `reviewCount` | Integer | Number of client reviews |
| `practiceAreas` | Array | Legal specializations |
| `location` | String | Office address (city, state, ZIP) |
| `phone` | String | Contact phone number |
| `email` | String | Contact email address |
| `website` | String | Attorney or firm website URL |
| `yearsLicensed` | Integer | Years practicing law |
| `barAdmissions` | Array | State bar and court admissions |
| `languages` | Array | Languages spoken |
| `profileUrl` | String | Avvo profile URL |
| `bio` | String | Professional biography |
| `education` | Array | Law school and undergraduate education |
| `awards` | Array | Professional recognition and awards |
| `scrapedAt` | String | ISO timestamp of data extraction |

---

## Export Formats

Download scraped data in multiple formats:

- **JSON** - Structured data for applications and APIs
- **CSV** - Spreadsheet-compatible format for Excel/Google Sheets
- **Excel** - Native .xlsx format with proper formatting
- **XML** - Enterprise system integration
- **RSS** - Feed-based updates
- **HTML** - Web-ready formatted output

---

## Use Cases

### Legal Research & Analytics

Analyze attorney demographics, practice area distribution, rating trends, and geographic coverage for market research and competitive intelligence.

### Lead Generation

Build targeted lists of attorneys by practice area and location for business development, referral networks, and partnership opportunities.

### Directory & Platform Development

Power legal directory websites, attorney matching platforms, and client referral services with comprehensive, up-to-date attorney data.

### Compliance & Background Checks

Verify attorney credentials, bar admissions, and professional standing for client vetting and compliance purposes.

### Marketing & Business Intelligence

Track attorney ratings, review trends, and service offerings to inform marketing strategies and identify market opportunities.

---

## Technical Architecture

### Multi-Strategy Extraction

The scraper employs three extraction methods with automatic fallback:

#### 1. JSON-LD Schema (Primary Method)

Extracts structured data from JSON-LD schema markup embedded in pages. This method provides the most reliable and complete data when available.

**Advantages:**
- Structured, validated data format
- Fast extraction without complex parsing
- High reliability and accuracy
- Minimal maintenance required

#### 2. Internal API Detection (Secondary Method)

Monitors network requests and embedded scripts to identify internal JSON APIs used by Avvo's frontend. When detected, data is extracted directly from API responses.

**Advantages:**
- 10-50x faster than HTML parsing
- Complete, structured data
- Handles dynamic content loading
- Bypasses client-side rendering delays

#### 3. HTML Parsing with Cheerio (Fallback Method)

Uses intelligent CSS selectors with multiple fallback patterns to extract data from rendered HTML. Ensures compatibility even when other methods fail.

**Advantages:**
- Works on any page structure
- Handles non-standard markup
- Multiple selector strategies
- Robust against site changes

### Cloudflare Bypass Technology

**Camoufox** - Advanced Firefox-based browser with built-in anti-detection capabilities:

- **Randomized Fingerprinting** - Dynamic OS, screen resolution, and timezone randomization
- **GeoIP Matching** - Automatic locale and location synchronization with proxy IP
- **Realistic Behavior** - Human-like navigation patterns and timing
- **Transparent Challenges** - Automatic Cloudflare challenge solving
- **Stealth Mode** - Undetectable browser automation

**Success Rate:** 99%+ Cloudflare bypass rate with residential proxies

### Performance Optimizations

| Optimization | Impact | Implementation |
|--------------|--------|----------------|
| Schema Extraction | 50x faster | JSON-LD parsing |
| API Detection | 10-25x faster | Network monitoring |
| Concurrent Requests | 3-5x faster | Parallel processing |
| Smart Caching | Reduced load | Context reuse |
| Profile Enrichment | Detailed data | Batch HTTP requests |

### Quality Assurance

- **Input Validation** - All input parameters validated before execution
- **Duplicate Detection** - URL-based deduplication across pagination
- **Error Handling** - Graceful fallbacks for failed requests
- **Data Cleaning** - Whitespace trimming and text normalization
- **Logging** - Comprehensive logging with sensitive data censoring
- **Debug Mode** - Automatic HTML capture when extraction fails

---

## Configuration Tips

### Maximizing Results

- Use specific practice areas for targeted results
- Enable `includeContactInfo` for complete profiles
- Set reasonable `maxLawyers` limits for faster runs
- Use Apify Proxy (residential) for best reliability
- Allow sufficient time for profile enrichment

### Performance Tuning

- **Small Runs** (<50 lawyers): ~2-3 minutes
- **Medium Runs** (50-200 lawyers): ~5-10 minutes
- **Large Runs** (200+ lawyers): ~15-30 minutes

Execution time increases when `includeContactInfo` is enabled due to individual profile page fetching.

### Practice Area Slugs

Common practice area slugs for search:

- `bankruptcy-debt` - Bankruptcy and debt relief
- `family` - Family law and divorce
- `criminal-defense` - Criminal defense
- `personal-injury` - Personal injury and accidents
- `real-estate` - Real estate law
- `business` - Business and corporate law
- `immigration` - Immigration law
- `employment` - Employment and labor law
- `estate-planning` - Estate planning and probate
- `traffic-tickets` - Traffic violations

---

## API Integration

### Node.js Example

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: 'YOUR_API_TOKEN' });

const run = await client.actor('YOUR_ACTOR_ID').call({
    practiceArea: 'bankruptcy-debt',
    state: 'al',
    city: 'birmingham',
    maxLawyers: 100,
    includeContactInfo: true
});

const { items } = await client.dataset(run.defaultDatasetId).listItems();
console.log(`Scraped ${items.length} lawyers`);
```

### Python Example

```python
from apify_client import ApifyClient

client = ApifyClient('YOUR_API_TOKEN')

run = client.actor('YOUR_ACTOR_ID').call(run_input={
    'practiceArea': 'family',
    'state': 'ca',
    'city': 'los-angeles',
    'maxLawyers': 50
})

items = client.dataset(run['defaultDatasetId']).list_items().items
print(f"Scraped {len(items)} lawyers")
```

### cURL Example

```bash
curl -X POST https://api.apify.com/v2/acts/YOUR_ACTOR_ID/runs \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "practiceArea": "criminal-defense",
    "state": "ny",
    "maxLawyers": 75
  }'
```

---

## Scheduling & Automation

### Automated Data Collection

Set up scheduled runs to maintain fresh attorney data:

1. Navigate to **Schedules** in Apify Console
2. Create new schedule (daily, weekly, custom cron)
3. Configure input parameters
4. Enable notifications for run completion
5. Set up webhooks for automated data processing

### Integration Options

- **Webhooks** - Trigger actions on completion
- **Zapier** - Connect to 5000+ apps without coding
- **Make** (Integromat) - Build complex automation workflows
- **Google Sheets** - Auto-export to spreadsheets
- **Slack/Discord** - Receive notifications with results
- **Email** - Automated reports via email

---

## Compliance & Ethics

### Responsible Data Collection

This scraper extracts publicly available attorney information from Avvo's directory for legitimate business purposes including:

- Legal research and market analysis
- Attorney credential verification
- Directory and platform development
- Lead generation for legal services

### Best Practices

- Use scraped data in accordance with applicable laws and regulations
- Respect attorney privacy and professional information
- Implement rate limiting and respectful scraping practices
- Use proxies to avoid server overload
- Do not scrape for spamming or harassment purposes
- Comply with data protection regulations (GDPR, CCPA, etc.)

---

## Troubleshooting

### No Lawyers Found

**Possible causes:**
- Invalid practice area slug or state code
- Cloudflare blocking (enable residential proxies)
- No lawyers match search criteria
- Page structure changed (check DEBUG_PAGE_HTML output)

**Solutions:**
- Verify practice area slug matches Avvo's URL format
- Use Apify residential proxies for better reliability
- Try different practice areas or locations
- Check Actor logs for specific error messages

### Incomplete Data

**Possible causes:**
- `includeContactInfo` disabled
- Profile pages blocked by Cloudflare
- Missing data on source profiles

**Solutions:**
- Enable `includeContactInfo` for complete profiles
- Use residential proxies for detail page fetching
- Some attorneys may not provide all information publicly

### Slow Performance

**Possible causes:**
- Large maxLawyers value
- Profile enrichment enabled
- Network latency

**Solutions:**
- Reduce maxLawyers for faster runs
- Disable `includeContactInfo` for listing-only data
- Use faster proxy groups if available

---

## FAQ

<details>
<summary><strong>How many lawyers can I scrape?</strong></summary>
<p>You can scrape unlimited lawyers by setting <code>maxLawyers</code> to 0. However, very large runs may take considerable time. We recommend setting reasonable limits for optimal performance.</p>
</details>

<details>
<summary><strong>Do I need proxies?</strong></summary>
<p>Yes, proxies are highly recommended. Avvo uses Cloudflare protection, and residential proxies provide the best success rate for bypassing these protections.</p>
</details>

<details>
<summary><strong>What practice areas are supported?</strong></summary>
<p>All practice areas available on Avvo are supported. Use the practice area slug from Avvo's URL (e.g., "bankruptcy-debt", "family", "criminal-defense").</p>
</details>

<details>
<summary><strong>Can I scrape specific cities?</strong></summary>
<p>Yes, use the <code>city</code> parameter to narrow results within a state. Leave empty to scrape the entire state.</p>
</details>

<details>
<summary><strong>How fresh is the data?</strong></summary>
<p>Data is extracted in real-time from Avvo during each run, ensuring the most up-to-date attorney information available.</p>
</details>

<details>
<summary><strong>What if an attorney doesn't have all fields?</strong></summary>
<p>Not all attorneys provide complete information on their profiles. Missing fields will be returned as empty strings, null, or empty arrays depending on the field type.</p>
</details>

<details>
<summary><strong>Can I scrape reviews and ratings?</strong></summary>
<p>Yes, the scraper extracts Avvo ratings and review counts. For detailed review text, enable <code>includeReviews</code> and <code>includeContactInfo</code>.</p>
</details>

---

## Support

### Need Help?

- **Documentation**: [Apify Docs](https://docs.apify.com)
- **Community**: [Discord Server](https://discord.com/invite/jyEM2PRvMU)
- **Issues**: Report bugs via Actor feedback form
- **Contact**: Reach out through Apify Console messaging

### Rate This Actor

If you find this scraper valuable for your legal research or business needs, please leave a rating and review on the Apify platform!

---

## License

This Actor is licensed under the Apache License 2.0. See the LICENSE file for details.

---

## Keywords

avvo scraper, lawyer scraper, attorney directory, legal data extraction, lawyer profiles, attorney search, legal services, law firm data, bar admissions, practice areas, attorney ratings, legal directory scraper, lawyer database, attorney leads

---

**Built for the Apify community** | [Get Started Now](https://console.apify.com) | [Documentation](https://docs.apify.com) | [Join Discord](https://discord.com/invite/jyEM2PRvMU)
