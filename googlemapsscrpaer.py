// Advanced Google Maps Scraper with Grid System
// A comprehensive implementation similar to Apify's Google Maps Scraper

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const crypto = require('crypto');

// Create necessary directories
['output', 'debug', 'cache'].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir);
});

// Set up the readline interface for user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Helper function for asking questions
function askQuestion(query) {
  return new Promise(resolve => rl.question(query, resolve));
}

// Helper function to hash strings for cache keys
function hashString(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

// Sleep function compatible with all Puppeteer versions
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Add waitForTimeout to page prototype if it doesn't exist (compatibility fix)
const addWaitForTimeoutIfNeeded = (page) => {
  if (!page.waitForTimeout) {
    page.waitForTimeout = function(timeout) {
      return sleep(timeout);
    };
  }
  return page;
};

// Utility class for logging
class Logger {
  constructor(level = 'info') {
    this.level = level;
    this.levels = {
      debug: 0,
      info: 1,
      warn: 2,
      error: 3
    };
    
    // Create log file
    this.logFile = path.join('debug', `scraper_${new Date().toISOString().replace(/:/g, '-')}.log`);
    fs.writeFileSync(this.logFile, `=== Google Maps Scraper Log ===\nStarted at: ${new Date().toISOString()}\n\n`);
  }
  
  log(level, message) {
    if (this.levels[level] >= this.levels[this.level]) {
      const timeStamp = new Date().toISOString();
      const formattedMessage = `[${timeStamp}] ${level.toUpperCase()}: ${message}`;
      
      // Print to console
      if (level === 'error') {
        console.error(formattedMessage);
      } else if (level === 'warn') {
        console.warn(formattedMessage);
      } else {
        console.log(formattedMessage);
      }
      
      // Write to log file
      fs.appendFileSync(this.logFile, formattedMessage + '\n');
    }
  }
  
  debug(message) { this.log('debug', message); }
  info(message) { this.log('info', message); }
  warn(message) { this.log('warn', message); }
  error(message) { this.log('error', message); }
}

// Consent handler for Google consent pages
class ConsentHandler {
  constructor(logger) {
    this.logger = logger;
    this.consentPatterns = [
      { urlPattern: "consent.google.com", severity: "high" },
      { urlPattern: "consent.youtube.com", severity: "high" },
      { urlPattern: "accounts.google.com", severity: "high" },
      { urlPattern: "_/consentview", severity: "medium" },
      { urlPattern: "consent_flow", severity: "medium" }
    ];
    
    this.debugDir = path.join(process.cwd(), 'debug');
  }
  
  async handleConsent(page, takeScreenshot = true) {
    try {
      // Make sure page has waitForTimeout
      page = addWaitForTimeoutIfNeeded(page);
      
      // Check if we're on a consent page
      const currentUrl = await page.url();
      
      // Detect consent page based on URL patterns
      let consentDetected = false;
      let severity = "low";
      
      for (const pattern of this.consentPatterns) {
        if (currentUrl.includes(pattern.urlPattern)) {
          consentDetected = true;
          severity = pattern.severity;
          break;
        }
      }
      
      // Also check for specific consent elements on the page
      if (!consentDetected) {
        try {
          const hasConsentElements = await page.evaluate(() => {
            // Check for common consent elements
            const consentTexts = ['cookies', 'consent', 'accept all', 'reject all'];
            const bodyText = document.body.innerText.toLowerCase();
            
            return consentTexts.some(text => bodyText.includes(text)) &&
                   (document.querySelector('button[jsaction*="accept"]') ||
                    document.querySelector('form button') ||
                    document.querySelector('button[jsname]'));
          });
          
          if (hasConsentElements) {
            consentDetected = true;
            this.logger.info("Detected consent page based on page content");
          }
        } catch (e) {
          this.logger.warn(`Error checking for consent elements: ${e.message}`);
        }
      }
      
      if (consentDetected) {
        this.logger.info(`⚠️ Detected consent page (${severity}): ${currentUrl}`);
        
        // Take a screenshot for debugging
        if (takeScreenshot) {
          const screenshotPath = path.join(this.debugDir, `consent_${Date.now()}.png`);
          try {
            await page.screenshot({ path: screenshotPath, fullPage: true });
            this.logger.info(`Saved consent page screenshot to ${screenshotPath}`);
          } catch (e) {
            this.logger.warn(`Error saving consent screenshot: ${e.message}`);
          }
        }
        
        // Try to handle the consent dialog with multiple methods
        let consentHandled = false;
        
        // Method 1: Try using preset selectors
        try {
          const selectors = [
            'button[jsname="tSZDoQd"]', // "Accept all" button jsname
            'button.tHlp8d',             // Common Google consent button
            'button[jsname="higCR"]',    // Another accept button
            'form button.VfPpkd-LgbsSe', // Material design button in form
            'form button:first-of-type', // First button in form
            'button.VfPpkd-LgbsSe',      // Material design button
            'div[role="dialog"] button'  // Any button in a dialog
          ];
          
          for (const selector of selectors) {
            try {
              const exists = await page.$(selector);
              if (exists) {
                this.logger.info(`Found consent button with selector: ${selector}`);
                await page.click(selector);
                await page.waitForTimeout(3000);
                consentHandled = true;
                break;
              }
            } catch (e) {
              this.logger.debug(`Error clicking selector ${selector}: ${e.message}`);
              continue;
            }
          }
        } catch (e) {
          this.logger.warn(`Error with selector-based consent handling: ${e.message}`);
        }
        
        // Method 2: Try JavaScript interaction
        if (!consentHandled) {
          try {
            consentHandled = await page.evaluate(() => {
              // First try to find buttons with Accept/Agree text
              const findButtonsByText = (searchText) => {
                searchText = searchText.toLowerCase();
                return Array.from(document.querySelectorAll('button, [role="button"]'))
                  .filter(el => {
                    const text = (el.textContent || '').toLowerCase();
                    return text.includes(searchText);
                  });
              };
              
              // Try multiple text variations
              let buttons = findButtonsByText('accept all');
              if (!buttons.length) buttons = findButtonsByText('agree');
              if (!buttons.length) buttons = findButtonsByText('accept');
              if (!buttons.length) buttons = findButtonsByText('ok');
              
              if (buttons.length) {
                buttons[0].click();
                console.log(`Clicked button with text: ${buttons[0].textContent.trim()}`);
                return true;
              }
              
              // Look for form buttons
              const formButtons = document.querySelectorAll('form button');
              if (formButtons.length) {
                formButtons[0].click();
                console.log('Clicked first form button');
                return true;
              }
              
              return false;
            });
            
            if (consentHandled) {
              this.logger.info('Clicked consent button using JavaScript');
              await page.waitForTimeout(3000);
            }
          } catch (e) {
            this.logger.warn(`Error with JavaScript-based consent handling: ${e.message}`);
          }
        }
        
        // Method 3: Try to parse the continue URL and navigate directly
        if (!consentHandled) {
          try {
            if (currentUrl.includes('consent.google.com')) {
              const urlParams = new URL(currentUrl).searchParams;
              const continueUrl = urlParams.get('continue');
              
              if (continueUrl) {
                this.logger.info(`Trying to bypass consent by navigating to: ${continueUrl}`);
                await page.goto(continueUrl, { 
                  waitUntil: 'networkidle2', 
                  timeout: 30000 
                });
                consentHandled = true;
              }
            }
          } catch (e) {
            this.logger.warn(`Error bypassing consent page: ${e.message}`);
          }
        }
        
        // Take another screenshot after handling
        if (takeScreenshot) {
          const afterScreenshotPath = path.join(this.debugDir, `after_consent_${Date.now()}.png`);
          try {
            await page.screenshot({ path: afterScreenshotPath, fullPage: true });
            this.logger.info(`Saved post-consent screenshot to ${afterScreenshotPath}`);
          } catch (e) {
            this.logger.warn(`Error saving post-consent screenshot: ${e.message}`);
          }
        }
        
        return consentHandled;
      }
      
      return false;
    } catch (e) {
      this.logger.error(`Error in consent handling: ${e.message}`);
      return false;
    }
  }
}

// Browser manager for handling multiple browser instances
class BrowserManager {
  constructor(options = {}, logger) {
    this.maxBrowsers = options.maxBrowsers || 1;
    this.browsers = [];
    this.browserHealth = {};
    this.browserInUse = {};
    this.isHeadless = options.isHeadless !== false;
    this.logger = logger;
    this.userDataDir = options.userDataDir || path.join(process.cwd(), 'user_data');
    
    // Create user data directory if it doesn't exist
    if (!fs.existsSync(this.userDataDir)) {
      fs.mkdirSync(this.userDataDir, { recursive: true });
    }
    
    this.logger.info(`Initialized browser manager (headless: ${this.isHeadless ? 'yes' : 'no'})`);
  }
  
  async initialize() {
    // Start with one browser
    await this._createBrowser();
    this.logger.info('Created initial browser instance');
  }
  
  async _createBrowser() {
    // Create a unique user data directory for this browser
    const browserId = this.browsers.length;
    const userDataDir = path.join(this.userDataDir, `browser_${browserId}_${Date.now()}`);
    
    if (!fs.existsSync(userDataDir)) {
      fs.mkdirSync(userDataDir, { recursive: true });
    }
    
    const browser = await puppeteer.launch({
      headless: this.isHeadless,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        `--user-data-dir=${userDataDir}`
      ],
      defaultViewport: null,
      ignoreHTTPSErrors: true
    });
    
    this.browsers.push(browser);
    this.browserHealth[browserId] = { errors: 0, pagesLoaded: 0 };
    this.browserInUse[browserId] = false;
    
    // Handle browser disconnection
    browser.on('disconnected', async () => {
      this.logger.warn(`Browser #${browserId} disconnected unexpectedly`);
      await this.reportError(browserId);
    });
    
    return browserId;
  }
  
  async getBrowser() {
    // Find an available browser
    for (let i = 0; i < this.browsers.length; i++) {
      if (!this.browserInUse[i]) {
        this.browserInUse[i] = true;
        return { browserId: i, browser: this.browsers[i] };
      }
    }
    
    // If no browser is available and we can create more
    if (this.browsers.length < this.maxBrowsers) {
      const browserId = await this._createBrowser();
      this.browserInUse[browserId] = true;
      return { browserId, browser: this.browsers[browserId] };
    }
    
    // Wait for a browser to become available
    return new Promise(resolve => {
      const checkInterval = setInterval(() => {
        for (let i = 0; i < this.browsers.length; i++) {
          if (!this.browserInUse[i]) {
            clearInterval(checkInterval);
            this.browserInUse[i] = true;
            resolve({ browserId: i, browser: this.browsers[i] });
            return;
          }
        }
      }, 500);
    });
  }
  
  releaseBrowser(browserId) {
    if (browserId in this.browserInUse) {
      this.browserInUse[browserId] = false;
      this.browserHealth[browserId].pagesLoaded++;
      this.logger.debug(`Released browser #${browserId} (total pages: ${this.browserHealth[browserId].pagesLoaded})`);
    }
  }
  
  async reportError(browserId) {
    if (browserId in this.browserHealth) {
      this.browserHealth[browserId].errors++;
      
      // If too many errors, recreate the browser
      if (this.browserHealth[browserId].errors >= 3) {
        this.logger.warn(`Browser #${browserId} has too many errors, recreating`);
        try {
          const oldBrowser = this.browsers[browserId];
          try {
            await oldBrowser.close();
          } catch (e) {
            this.logger.warn(`Error closing browser #${browserId}: ${e.message}`);
          }
          
          // Create a unique user data directory for the new browser
          const userDataDir = path.join(this.userDataDir, `browser_${browserId}_${Date.now()}`);
          
          if (!fs.existsSync(userDataDir)) {
            fs.mkdirSync(userDataDir, { recursive: true });
          }
          
          const newBrowser = await puppeteer.launch({
            headless: this.isHeadless,
            args: [
              '--no-sandbox',
              '--disable-setuid-sandbox',
              '--disable-dev-shm-usage',
              '--disable-web-security',
              '--disable-features=IsolateOrigins,site-per-process',
              `--user-data-dir=${userDataDir}`
            ],
            defaultViewport: null,
            ignoreHTTPSErrors: true
          });
          
          this.browsers[browserId] = newBrowser;
          this.browserHealth[browserId] = { errors: 0, pagesLoaded: 0 };
          this.browserInUse[browserId] = false;
          this.logger.info(`Recreated browser #${browserId}`);
          
          // Handle browser disconnection for new browser
          newBrowser.on('disconnected', async () => {
            this.logger.warn(`Browser #${browserId} disconnected unexpectedly`);
            await this.reportError(browserId);
          });
        } catch (e) {
          this.logger.error(`Error recreating browser #${browserId}: ${e.message}`);
        }
      }
    }
  }
  
  async closeAll() {
    for (let i = 0; i < this.browsers.length; i++) {
      try {
        await this.browsers[i].close();
        this.logger.info(`Closed browser #${i}`);
      } catch (e) {
        this.logger.warn(`Error closing browser #${i}: ${e.message}`);
      }
    }
    
    this.browsers = [];
    this.browserHealth = {};
    this.browserInUse = {};
    this.logger.info('Closed all browsers');
  }
}

// Data cache manager
class DataCache {
  constructor(options = {}, logger) {
    this.enabled = options.enabled !== false;
    this.maxAgeHours = options.maxAgeHours || 24;
    this.maxAgeSeconds = this.maxAgeHours * 3600;
    this.cacheDir = path.join(process.cwd(), 'cache');
    this.logger = logger;
    
    // Create cache directory if it doesn't exist
    if (!fs.existsSync(this.cacheDir)) {
      fs.mkdirSync(this.cacheDir, { recursive: true });
    }
    
    this.logger.info(`Initialized data cache (enabled: ${this.enabled ? 'yes' : 'no'}, max age: ${this.maxAgeHours}h)`);
    
    // Clear old cache entries on startup
    if (this.enabled) {
      this._clearOldCache();
    }
  }
  
  _getCachePath(cacheKey) {
    const hashedKey = hashString(cacheKey);
    return path.join(this.cacheDir, `${hashedKey}.json`);
  }
  
  _clearOldCache() {
    const now = Date.now();
    let count = 0;
    
    fs.readdirSync(this.cacheDir).forEach(file => {
      if (file.endsWith('.json')) {
        const filePath = path.join(this.cacheDir, file);
        try {
          const stats = fs.statSync(filePath);
          if (now - stats.mtimeMs > this.maxAgeSeconds * 1000) {
            fs.unlinkSync(filePath);
            count++;
          }
        } catch (e) {
          // Ignore file errors
        }
      }
    });
    
    if (count > 0) {
      this.logger.info(`Cleared ${count} old cache entries`);
    }
  }
  
  get(cacheKey) {
    if (!this.enabled) {
      return null;
    }
    
    const cachePath = this._getCachePath(cacheKey);
    
    try {
      if (fs.existsSync(cachePath)) {
        // Check if cache is too old
        const stats = fs.statSync(cachePath);
        if (Date.now() - stats.mtimeMs > this.maxAgeSeconds * 1000) {
          fs.unlinkSync(cachePath);
          return null;
        }
        
        // Read cache entry
        const data = JSON.parse(fs.readFileSync(cachePath, 'utf-8'));
        this.logger.info(`Cache hit for ${cacheKey.substring(0, 30)}...`);
        return data;
      }
    } catch (e) {
      this.logger.warn(`Error reading from cache: ${e.message}`);
    }
    
    return null;
  }
  
  set(cacheKey, value) {
    if (!this.enabled) {
      return;
    }
    
    const cachePath = this._getCachePath(cacheKey);
    
    try {
      fs.writeFileSync(cachePath, JSON.stringify(value), 'utf-8');
      this.logger.info(`Cached data for ${cacheKey.substring(0, 30)}...`);
    } catch (e) {
      this.logger.warn(`Error writing to cache: ${e.message}`);
    }
  }
  
  invalidate(cacheKey) {
    if (!this.enabled) {
      return;
    }
    
    const cachePath = this._getCachePath(cacheKey);
    
    try {
      if (fs.existsSync(cachePath)) {
        fs.unlinkSync(cachePath);
        this.logger.info(`Invalidated cache for ${cacheKey.substring(0, 30)}...`);
      }
    } catch (e) {
      this.logger.warn(`Error invalidating cache: ${e.message}`);
    }
  }
}

// Grid-based scraper
class GoogleMapsGridScraper {
  constructor(options = {}) {
    // Initialize logger
    this.logger = new Logger(options.logLevel || 'info');
    
    // Core settings
    this.searchTerm = options.searchTerm || '';
    this.location = options.location || '';
    this.maxResults = options.maxResults || 100000;
    this.maxConcurrency = options.maxConcurrency || 10;
    
    // Grid settings
    this.useGridScraping = options.useGridScraping !== false;
    this.initialGridSize = options.initialGridSize || 2; // 2x2 grid
    this.maxGridDivisions = options.maxGridDivisions || 4; // Up to 4x4 grid for each cell
    this.resultsThreshold = options.resultsThreshold || 40; // If more than this, subdivide
    this.minCellSize = options.minCellSize || 0.001; // Approx 111 meters
    
    // Browser settings
    this.isHeadless = options.isHeadless !== false;
    
    // Initialize components
    this.browserManager = new BrowserManager({
      maxBrowsers: this.maxConcurrency,
      isHeadless: this.isHeadless,
      userDataDir: path.join(process.cwd(), 'user_data')
    }, this.logger);
    
    this.dataCache = new DataCache({
      enabled: options.useCaching !== false,
      maxAgeHours: options.cacheMaxAgeHours || 24
    }, this.logger);
    
    this.consentHandler = new ConsentHandler(this.logger);
    
    // Results and state
    this.results = [];
    this.processedUrls = new Set();
    this.gridCells = [];
    this.taskQueue = [];
    
    // Create output directory if it doesn't exist
    if (!fs.existsSync('./output')) {
      fs.mkdirSync('./output', { recursive: true });
    }
    
    this.logger.info('Google Maps Grid Scraper initialized');
  }
  
  // Initialize with user input
  async initializeWithUserInput() {
    this.logger.info('==== Google Maps Grid Scraper ====');
    
    // Get search parameters from user
    this.searchTerm = await askQuestion('What do you want to search for? (e.g., "restaurants"): ');
    this.location = await askQuestion('Location to search in? (e.g., "San Francisco, CA"): ');
    this.maxResults = parseInt(await askQuestion('Maximum number of results to collect (e.g., 100): '), 10);
    
    const headlessMode = await askQuestion('Run in headless mode? (y/n): ');
    this.isHeadless = headlessMode.toLowerCase() === 'y';
    this.browserManager.isHeadless = this.isHeadless;
    
    const useGrid = await askQuestion('Use grid-based scraping? (y/n): ');
    this.useGridScraping = useGrid.toLowerCase() === 'y';
    
    if (this.useGridScraping) {
      const gridSize = await askQuestion('Initial grid size (2 for 2x2, 3 for 3x3, etc.): ');
      this.initialGridSize = parseInt(gridSize, 10) || 2;
    }
    
    const cacheEnabled = await askQuestion('Enable caching? (y/n): ');
    this.dataCache.enabled = cacheEnabled.toLowerCase() === 'y';
    
    // Initialize browser
    await this.browserManager.initialize();
    
    this.logger.info('\nScraper initialized with these settings:');
    this.logger.info(`- Search Term: ${this.searchTerm}`);
    this.logger.info(`- Location: ${this.location}`);
    this.logger.info(`- Max Results: ${this.maxResults}`);
    this.logger.info(`- Headless Mode: ${this.isHeadless ? 'Yes' : 'No'}`);
    this.logger.info(`- Grid Scraping: ${this.useGridScraping ? 'Yes' : 'No'}`);
    this.logger.info(`- Caching: ${this.dataCache.enabled ? 'Enabled' : 'Disabled'}`);
    
    return this;
  }
  
  // Generate a grid based on geographic coordinates
  generateGrid(boundingBox, divisions) {
    const cells = [];
    const { north, south, east, west } = boundingBox;
    
    const latStep = (north - south) / divisions;
    const lngStep = (east - west) / divisions;
    
    for (let latIdx = 0; latIdx < divisions; latIdx++) {
      const cellSouth = south + (latIdx * latStep);
      const cellNorth = south + ((latIdx + 1) * latStep);
      
      for (let lngIdx = 0; lngIdx < divisions; lngIdx++) {
        const cellWest = west + (lngIdx * lngStep);
        const cellEast = west + ((lngIdx + 1) * lngStep);
        
        cells.push({
          north: cellNorth,
          south: cellSouth,
          east: cellEast,
          west: cellWest,
          center: {
            lat: (cellNorth + cellSouth) / 2,
            lng: (cellEast + cellWest) / 2
          }
        });
      }
    }
    
    return cells;
  }
  
  // Main scraping method
  async scrape() {
    if (this.useGridScraping) {
      await this.gridBasedScrape();
    } else {
      await this.standardScrape();
    }
    
    this.logger.info(`\nScraping completed. Collected ${this.results.length} results.`);
  }
  
  // Normal scraping approach
  async standardScrape() {
    const cacheKey = `${this.searchTerm}_${this.location}_standard`;
    
    // Try to get results from cache first
    if (this.dataCache.enabled) {
      const cachedResults = this.dataCache.get(cacheKey);
      if (cachedResults) {
        this.logger.info(`Found ${cachedResults.length} results in cache`);
        this.results = cachedResults;
        return;
      }
    }
    
    // Get a browser instance
    const { browserId, browser } = await this.browserManager.getBrowser();
    let page = null;
    
    try {
      this.logger.info(`Using browser #${browserId} for standard scraping`);
      
      // Create a new page
      page = await browser.newPage();
      page = addWaitForTimeoutIfNeeded(page);
      
      // Set viewport to a typical desktop size
      await page.setViewport({ width: 1366, height: 768 });
      
      // Set user agent to a common browser
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
      );
      
      // Construct search URL
      const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(this.searchTerm)}+${encodeURIComponent(this.location)}`;
      this.logger.info(`\nNavigating to: ${searchUrl}`);
      
      // Go to Google Maps with the search query
      await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 60000 });
      
      // Handle any consent dialogs
      await this.consentHandler.handleConsent(page);
      
      // Wait a bit for results to fully load
      await page.waitForTimeout(3000);
      
      // Take screenshot of search results
      const screenshotPath = path.join('debug', `search_results_${Date.now()}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      this.logger.info(`Saved screenshot of search results to ${screenshotPath}`);
      
      // Start collecting results
      this.logger.info('\nStarting to collect results...');
      
      // Scroll and collect results
      let collectedCount = 0;
      let previousResultCount = 0;
      let scrollAttempts = 0;
      let consecutiveEmptyScrolls = 0;
      const maxScrollAttempts = 50;
      
      while (collectedCount < this.maxResults && scrollAttempts < maxScrollAttempts && consecutiveEmptyScrolls < 3) {
        // Extract place data
        const newResults = await this.extractPlaceData(page);
        
        // Filter out duplicates and add to results
        const initialCount = this.results.length;
        
        for (const place of newResults) {
          // Check if we already have this place
          const isDuplicate = this.processedUrls.has(place.url || place.name);
          
          if (!isDuplicate) {
            this.processedUrls.add(place.url || place.name);
            this.results.push(place);
          }
        }
        
        collectedCount = this.results.length;
        const newlyAdded = collectedCount - initialCount;
        
        this.logger.info(`Found ${newResults.length} places, ${newlyAdded} new (total: ${collectedCount}/${this.maxResults})`);
        
        // If no new results after scrolling, we might be at the end
        if (newlyAdded === 0) {
          consecutiveEmptyScrolls++;
          this.logger.info(`No new results found after scrolling (attempt ${consecutiveEmptyScrolls}/3)`);
          
          if (consecutiveEmptyScrolls >= 3) {
            this.logger.info('Reached end of results after multiple empty scrolls');
            break;
          }
          
          // Try clicking "More results" button if it exists
          try {
            const moreResultsButton = await page.$('button[jsaction*="moreResults"], button:has-text("More results")');
            if (moreResultsButton) {
              this.logger.info('Found "More results" button. Clicking it...');
              await moreResultsButton.click();
              await page.waitForTimeout(3000);
              consecutiveEmptyScrolls = 0; // Reset counter after clicking "More results"
            }
          } catch (e) {
            this.logger.warn(`Error clicking more results button: ${e.message}`);
          }
        } else {
          consecutiveEmptyScrolls = 0; // Reset counter if we found new results
        }
        
        previousResultCount = collectedCount;
        
        // If we have enough results, stop scrolling
        if (collectedCount >= this.maxResults) {
          break;
        }
        
        // Scroll down in the results panel
        this.logger.info('Scrolling for more results...');
        
        try {
          await this.scrollResultsPanel(page);
          
          // Wait for potential new results to load
          await page.waitForTimeout(2000);
          scrollAttempts++;
        } catch (e) {
          this.logger.error(`Error during scrolling: ${e.message}`);
          await this.browserManager.reportError(browserId);
        }
      }
      
      // Cache the results if enabled
      if (this.dataCache.enabled && this.results.length > 0) {
        this.dataCache.set(cacheKey, this.results);
      }
      
    } catch (error) {
      this.logger.error(`Error during standard scraping: ${error.message}`);
      
      // Report browser error
      await this.browserManager.reportError(browserId);
      
      // Take screenshot on error
      if (page) {
        try {
          const errorScreenshotPath = path.join('debug', `error_${Date.now()}.png`);
          await page.screenshot({ path: errorScreenshotPath, fullPage: true });
          this.logger.info(`Error screenshot saved to ${errorScreenshotPath}`);
        } catch (e) {
          this.logger.warn(`Error taking screenshot: ${e.message}`);
        }
      }
    } finally {
      // Close the page and release the browser
      if (page) {
        await page.close();
      }
      this.browserManager.releaseBrowser(browserId);
    }
  }
  
  // Grid-based scraping approach
  async gridBasedScrape() {
    this.logger.info('Starting grid-based scraping...');
    
    try {
      // First, get the bounding box for the location
      const boundingBox = await this.getBoundingBoxForLocation(this.location);
      if (!boundingBox) {
        this.logger.error("Couldn't determine bounding box for location. Falling back to standard scraping.");
        return await this.standardScrape();
      }
      
      this.logger.info(`Location bounding box: ${JSON.stringify(boundingBox)}`);
      
      // Generate the initial grid
      this.gridCells = this.generateGrid(boundingBox, this.initialGridSize);
      this.logger.info(`Created initial grid with ${this.gridCells.length} cells`);
      
      // Queue up all grid cells as search tasks
      for (const cell of this.gridCells) {
        this.taskQueue.push({
          type: 'searchCell',
          cell: cell,
          searchTerm: this.searchTerm,
          depth: 0
        });
      }
      
      // Process the task queue
      await this.processGridTaskQueue();
      
    } catch (error) {
      this.logger.error(`Error in grid-based scraping: ${error.message}`);
      
      // If grid scraping fails, try standard scraping
      this.logger.info('Falling back to standard scraping...');
      await this.standardScrape();
    }
  }
  
  // Process grid-based task queue
  async processGridTaskQueue() {
    this.logger.info(`Starting to process ${this.taskQueue.length} grid tasks...`);
    
    // Process tasks until queue is empty or we have enough results
    while (this.taskQueue.length > 0 && this.results.length < this.maxResults) {
      // Process tasks based on concurrency
      const tasks = [];
      
      while (this.taskQueue.length > 0 && 
             tasks.length < this.maxConcurrency &&
             this.results.length < this.maxResults) {
        tasks.push(this.processGridTask(this.taskQueue.shift()));
      }
      
      await Promise.all(tasks);
      
      // Output progress
      this.logger.info(`Grid progress: ${this.results.length}/${this.maxResults} results collected, ${this.taskQueue.length} tasks remaining`);
    }
    
    this.logger.info('All grid tasks processed.');
    
    // Cache the results if enabled
    if (this.dataCache.enabled && this.results.length > 0) {
      const cacheKey = `${this.searchTerm}_${this.location}_grid`;
      this.dataCache.set(cacheKey, this.results);
    }
  }
  
  // Process a single grid task
  async processGridTask(task) {
    if (task.type === 'searchCell') {
      await this.searchGridCell(task.cell, task.searchTerm, task.depth);
    } else if (task.type === 'getDetails') {
      await this.getPlaceDetails(task.placeId, task.baseData);
    }
  }
  
  // Search within a grid cell
  async searchGridCell(cell, searchTerm, depth) {
    // Skip if we already have enough results
    if (this.results.length >= this.maxResults) {
      return;
    }
    
    this.logger.info(`Searching cell at ${cell.center.lat.toFixed(6)}, ${cell.center.lng.toFixed(6)} (depth ${depth})`);
    
    // Get a browser instance
    const { browserId, browser } = await this.browserManager.getBrowser();
    let page = null;
    
    try {
      // Create a new page
      page = await browser.newPage();
      page = addWaitForTimeoutIfNeeded(page);
      
      // Set viewport to a typical desktop size
      await page.setViewport({ width: 1366, height: 768 });
      
      // Set user agent to a common browser
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
      );
      
      // Navigate to Google Maps centered on the cell
      // Use the '@' syntax to specify coordinates
      const url = `https://www.google.com/maps/search/${encodeURIComponent(searchTerm)}/@${cell.center.lat},${cell.center.lng},15z`;
      
      this.logger.info(`Navigating to: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
      
      // Handle any consent dialogs
      await this.consentHandler.handleConsent(page);
      
      // Wait for results to load
      await page.waitForTimeout(3000);
      
      // Take a screenshot for debugging
      if (depth === 0) { // Only for top-level cells to avoid too many screenshots
        const cellScreenshotPath = path.join('debug', `cell_${cell.center.lat.toFixed(6)}_${cell.center.lng.toFixed(6)}.png`);
        await page.screenshot({ path: cellScreenshotPath, fullPage: true });
        this.logger.debug(`Saved cell screenshot to ${cellScreenshotPath}`);
      }
      
      // Scroll to load all results in this view
      let resultCount = 0;
      try {
        resultCount = await this.scrollAndCountResults(page);
      } catch (e) {
        this.logger.warn(`Error scrolling for results: ${e.message}`);
      }
      
      this.logger.info(`Found ${resultCount} results in this cell`);
      
      // Check if we need to subdivide this cell
      if (resultCount >= this.resultsThreshold && depth < this.maxGridDivisions) {
        this.logger.info(`Cell has ${resultCount} results, subdividing...`);
        
        // Calculate cell size to check minimum size constraint
        const cellWidth = cell.east - cell.west;
        const cellHeight = cell.north - cell.south;
        
        if (cellWidth > this.minCellSize * 2 && cellHeight > this.minCellSize * 2) {
          // Subdivide into a more detailed grid (2x2 = 4 subcells)
          const subCells = this.generateGrid(cell, 2);
          
          // Add subcells to the task queue
          for (const subCell of subCells) {
            this.taskQueue.push({
              type: 'searchCell',
              cell: subCell,
              searchTerm: searchTerm,
              depth: depth + 1
            });
          }
          
          // Don't process results from this cell to avoid duplicates
          this.logger.info(`Subdivided cell into ${subCells.length} subcells`);
          
          await page.close();
          this.browserManager.releaseBrowser(browserId);
          return;
        } else {
          this.logger.info(`Cell reached minimum size, processing results despite density`);
        }
      }
      
      // Extract place data from search results
      const places = await this.extractPlaceData(page);
      
      // Process each place
      for (const place of places) {
        // Skip already processed places
        if (this.processedUrls.has(place.url || place.name)) {
          continue;
        }
        
        this.processedUrls.add(place.url || place.name);
        this.results.push(place);
        
        // Check if we've reached the result limit
        if (this.results.length >= this.maxResults) {
          break;
        }
      }
      
    } catch (error) {
      this.logger.error(`Error searching cell: ${error.message}`);
      await this.browserManager.reportError(browserId);
    } finally {
      // Close the page and release the browser
      if (page) {
        await page.close();
      }
      this.browserManager.releaseBrowser(browserId);
    }
  }
  
  // Get bounding box for a location
  async getBoundingBoxForLocation(location) {
    // First try to get from cache
    const cacheKey = `bbox_${location}`;
    if (this.dataCache.enabled) {
      const cachedBoundingBox = this.dataCache.get(cacheKey);
      if (cachedBoundingBox) {
        return cachedBoundingBox;
      }
    }
    
    // Get a browser instance
    const { browserId, browser } = await this.browserManager.getBrowser();
    let page = null;
    
    try {
      this.logger.info(`Getting bounding box for location: ${location}`);
      
      // Create a new page
      page = await browser.newPage();
      page = addWaitForTimeoutIfNeeded(page);
      
      // Go to Google Maps with the location
      await page.goto(`https://www.google.com/maps/place/${encodeURIComponent(location)}`, {
        waitUntil: 'networkidle2',
        timeout: 60000
      });
      
      // Handle any consent dialogs
      await this.consentHandler.handleConsent(page);
      
      // Wait for the map to load
      await page.waitForTimeout(3000);
      
      // Get the current URL which contains viewport coordinates
      const url = await page.url();
      
      // Try to extract viewport coordinates from URL
      const viewportMatch = url.match(/@(-?\d+\.\d+),(-?\d+\.\d+),(\d+z)/);
      
      if (!viewportMatch) {
        this.logger.warn(`Couldn't extract viewport coordinates from URL: ${url}`);
        return null;
      }
      
      const centerLat = parseFloat(viewportMatch[1]);
      const centerLng = parseFloat(viewportMatch[2]);
      const zoom = parseInt(viewportMatch[3].replace('z', ''), 10);
      
      // Calculate a bounding box based on the center and zoom
      // This is an approximation - each zoom level doubles the scale
      const latDelta = 180 / Math.pow(2, zoom - 1);
      const lngDelta = 360 / Math.pow(2, zoom - 1);
      
      const boundingBox = {
        north: centerLat + latDelta/2,
        south: centerLat - latDelta/2,
        east: centerLng + lngDelta/2,
        west: centerLng - lngDelta/2
      };
      
      // Cache the result
      if (this.dataCache.enabled) {
        this.dataCache.set(cacheKey, boundingBox);
      }
      
      return boundingBox;
      
    } catch (error) {
      this.logger.error(`Error getting bounding box: ${error.message}`);
      await this.browserManager.reportError(browserId);
      return null;
    } finally {
      // Close the page and release the browser
      if (page) {
        await page.close();
      }
      this.browserManager.releaseBrowser(browserId);
    }
  }
  
  // Scroll results panel and count results
  async scrollAndCountResults(page) {
    let previousResultCount = 0;
    let currentResultCount = 0;
    let scrollAttempts = 0;
    const maxScrollAttempts = 20;
    
    try {
      while (scrollAttempts < maxScrollAttempts) {
        // Count current results
        currentResultCount = await page.evaluate(() => {
          const resultElements = document.querySelectorAll('div[role="article"], .hfpxzc, .section-result');
          return resultElements.length;
        });
        
        // If no new results after scrolling, we're done
        if (currentResultCount > 0 && currentResultCount === previousResultCount) {
          break;
        }
        
        previousResultCount = currentResultCount;
        
        // Scroll down in the results panel
        await this.scrollResultsPanel(page);
        
        // Wait for potential new results to load
        await page.waitForTimeout(2000);
        scrollAttempts++;
      }
      
      return currentResultCount;
    } catch (error) {
      this.logger.error(`Error scrolling for results: ${error.message}`);
      return 0;
    }
  }
  
  // Extract place data from the page
  async extractPlaceData(page) {
    return await page.evaluate(() => {
      const places = [];
      
      // Try different selectors for result items
      let resultItems = [];
      
      // Modern layout (articles)
      const modernItems = document.querySelectorAll('div[role="article"]');
      if (modernItems.length > 0) {
        resultItems = Array.from(modernItems);
      } else {
        // Legacy layout
        const legacyItems = document.querySelectorAll('.hfpxzc, .section-result');
        if (legacyItems.length > 0) {
          resultItems = Array.from(legacyItems);
        } else {
          // Generic approach, look for containers with multiple data points
          const genericItems = Array.from(document.querySelectorAll('div'))
            .filter(el => {
              // Check if the div contains typical business listing elements
              return (
                el.querySelector('a[href*="maps/place"]') && 
                el.querySelectorAll('div').length > 5 &&
                el.textContent.length > 10
              );
            });
          resultItems = genericItems;
        }
      }
      
      for (const item of resultItems) {
        try {
          // Data extraction logic
          let name = '';
          let address = '';
          let rating = null;
          let reviewCount = null;
          let url = '';
          let type = '';
          let placeId = '';
          
          // Extract name
          const nameElement = item.querySelector('div[role="heading"], h3, .section-result-title');
          if (nameElement) {
            name = nameElement.textContent.trim();
          } else {
            // Try to find the first prominent text
            const allText = Array.from(item.querySelectorAll('div'))
              .map(el => el.textContent.trim())
              .filter(text => text.length > 0);
              
            if (allText.length > 0) {
              name = allText[0];
            }
          }
          
          // If we couldn't find a name, skip this item
          if (!name) continue;
          
          // Extract address (usually 2nd or 3rd text element)
          const addressElements = item.querySelectorAll('div:not([role="heading"])');
          if (addressElements.length > 1) {
            // Try to find address-like text (contains numbers and commas)
            for (let i = 1; i < Math.min(5, addressElements.length); i++) {
              const text = addressElements[i].textContent.trim();
              if (text && /\d+.*(,|St|Ave|Rd|Blvd|Dr|Lane|Way)/.test(text)) {
                address = text;
                break;
              }
            }
            
            // If no specific address pattern was found, use the second text element
            if (!address && addressElements.length > 1) {
              address = addressElements[1].textContent.trim();
            }
          }
          
          // Extract rating
          const ratingElement = item.querySelector('[aria-label*="stars"], [aria-label*="rating"]');
          if (ratingElement) {
            const ratingText = ratingElement.getAttribute('aria-label') || '';
            const ratingMatch = ratingText.match(/([0-9.]+)/);
            if (ratingMatch) {
              rating = parseFloat(ratingMatch[1]);
            }
          }
          
          // Extract review count
          const reviewElement = item.querySelector('[aria-label*="reviews"], [aria-label*="Review"]');
          if (reviewElement) {
            const reviewText = reviewElement.getAttribute('aria-label') || '';
            const reviewMatch = reviewText.match(/([0-9,]+)/);
            if (reviewMatch) {
              reviewCount = parseInt(reviewMatch[1].replace(/,/g, ''));
            }
          }
          
          // Extract URL
          const linkElement = item.querySelector('a[href*="maps/place"]');
          if (linkElement) {
            url = linkElement.href;
            
            // Extract place ID from URL
            const placeIdMatch = url.match(/place_id=([^&]+)/);
            if (placeIdMatch) {
              placeId = placeIdMatch[1];
            }
          }
          
          // Extract business type
          const typeElements = Array.from(item.querySelectorAll('div')).slice(0, 5);
          for (const el of typeElements) {
            const text = el.textContent.trim();
            // Business types are typically short phrases without numbers
            if (text && text !== name && text !== address && !text.match(/[0-9.]+/) && text.length < 50) {
              type = text;
              break;
            }
          }
          
          // Add place to results
          if (name) {
            places.push({
              name,
              address,
              type,
              rating,
              reviewCount,
              url,
              placeId
            });
          }
        } catch (err) {
          console.error('Error extracting place data:', err);
        }
      }
      
      return places;
    });
  }
  
  // Scroll the results panel to load more results
  async scrollResultsPanel(page) {
    try {
      await page.evaluate(() => {
        // Try different scroll containers
        const scrollContainers = [
          document.querySelector('div[role="feed"]'),
          document.querySelector('.section-layout.section-scrollbox'),
          document.querySelector('div[jsaction*="mouseover:pane"]'),
          document.querySelector('div.section-listbox'),
          document.querySelector('div[role="main"]')
        ];
        
        for (const container of scrollContainers) {
          if (container) {
            const previousHeight = container.scrollHeight;
            container.scrollTop = container.scrollHeight;
            return { scrolled: true, container: 'panel' };
          }
        }
        
        // If no scroll container found, scroll the whole page
        window.scrollTo(0, document.body.scrollHeight);
        return { scrolled: true, container: 'page' };
      });
      
      // Wait briefly for content to load
      await page.waitForTimeout(1500);
      
      return true;
    } catch (error) {
      this.logger.error(`Error scrolling: ${error.message}`);
      return false;
    }
  }
  
  // Save results to file
  async saveResults() {
    if (this.results.length === 0) {
      this.logger.info('No results to save');
      return;
    }
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputDir = './output';
    
    // Save as JSON
    const jsonPath = path.join(outputDir, `google_maps_${timestamp}.json`);
    fs.writeFileSync(jsonPath, JSON.stringify(this.results, null, 2));
    this.logger.info(`Results saved as JSON: ${jsonPath}`);
    
    // Save as CSV
    const csvPath = path.join(outputDir, `google_maps_${timestamp}.csv`);
    this.saveAsCSV(csvPath);
    this.logger.info(`Results saved as CSV: ${csvPath}`);
  }
  
  // Save results as CSV
  saveAsCSV(filePath) {
    if (this.results.length === 0) {
      this.logger.info('No results to save as CSV');
      return;
    }
    
    // Get all unique keys from results
    const allKeys = new Set();
    this.results.forEach(result => {
      Object.keys(result).forEach(key => {
        // Skip undefined values
        if (result[key] !== undefined) {
          allKeys.add(key);
        }
      });
    });
    
    // Convert to array and prioritize important fields
    const priorityFields = ['name', 'address', 'type', 'rating', 'reviewCount', 'url', 'placeId'];
    const otherFields = [...allKeys].filter(key => !priorityFields.includes(key));
    const headers = [...priorityFields, ...otherFields];
    
    // Create CSV content
    let csvContent = headers.join(',') + '\n';
    
    this.results.forEach(result => {
      const row = headers.map(header => {
        const value = result[header];
        
        if (value === undefined || value === null) {
          return '';
        }
        
        // Escape and quote strings with commas
        if (typeof value === 'string') {
          if (value.includes(',') || value.includes('"') || value.includes('\n')) {
            return `"${value.replace(/"/g, '""')}"`;
          }
          return value;
        }
        
        return String(value);
      });
      
      csvContent += row.join(',') + '\n';
    });
    
    fs.writeFileSync(filePath, csvContent);
  }
  
  // Run the complete scraping process
  async run() {
    try {
      // Initialize browser manager
      await this.browserManager.initialize();
      
      // Scrape
      await this.scrape();
      
      // Save results
      await this.saveResults();
      
      this.logger.info('\nScraping completed successfully!');
      return this.results;
    } catch (error) {
      this.logger.error(`Error running scraper: ${error.message}`);
      return [];
    } finally {
      // Close all browsers
      await this.browserManager.closeAll();
      rl.close();
    }
  }
}

// Main function to run the scraper
async function runGoogleMapsScraper() {
  try {
    const scraper = new GoogleMapsGridScraper();
    await scraper.initializeWithUserInput();
    await scraper.run();
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Start the scraper if this is the main script
if (require.main === module) {
  runGoogleMapsScraper();
}

// Export the scraper class for use in other scripts
module.exports = { GoogleMapsGridScraper };
