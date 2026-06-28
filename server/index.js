import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import { fileURLToPath } from 'url';
import fetch from 'node-fetch';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// Behind Render's reverse proxy: trust exactly one hop so req.ip is the real
// client IP (needed for the /chat rate limiter to bucket per-user, not globally).
app.set('trust proxy', 1);

// Resolve the OpenAI API key once at startup: env var first, then Render's
// secret-file path, then a local api_key.txt for development.
const OPENAI_API_KEY = (() => {
  if (process.env.OPENAI_API_KEY) return process.env.OPENAI_API_KEY.trim();
  const candidates = ['/etc/secrets/api_key.txt', path.resolve(__dirname, '..', 'api_key.txt')];
  for (const p of candidates) {
    try {
      const k = fs.readFileSync(p, 'utf8').trim();
      if (k) return k;
    } catch {}
  }
  return null;
})();

// Restrict CORS to known origins. Production serves the client same-origin
// (so no CORS is needed there); file:// dev sends a null Origin, which we allow.
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || 'http://localhost:3000')
  .split(',').map((s) => s.trim()).filter(Boolean);
app.use(cors({
  origin: (origin, cb) => {
    // No Origin = same-origin or a non-browser client (curl); 'null' = a file:// dev page.
    if (!origin || origin === 'null' || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error('Not allowed by CORS'));
  },
  methods: ['GET', 'POST'],
}));

app.use(express.json({ limit: '1mb' }));

// Finalize body-parser errors: it throws a SyntaxError (status 400) for
// malformed JSON, and an 'entity.too.large' error for bodies over the limit.
app.use((err, req, res, next) => {
  if (!err) return next();
  if (res.headersSent) return next(err);
  if (err.type === 'entity.too.large') {
    return res.status(413).json({ error: 'Request body too large' });
  }
  if (err instanceof SyntaxError || err.status === 400) {
    return res.status(400).json({ error: 'Invalid JSON in request body' });
  }
  return next(err);
});

// Serve static client
app.use('/', express.static(path.join(__dirname, 'public')));

// Serve minified HTML as the main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.min.html'));
});

// Redirect requests for index.html to the minified version
app.get('/index.html', (req, res) => {
  res.redirect('/');
});

// Data in-memory
/** @type {Array<{
 * id: string,
 * name: string,
 * address: string,
 * city: string,
 * borough: string,
 * zip: string,
 * county: string,
 * storeType: string,
 * isHealthyStore: boolean,
 * aiScore?: number,
 * aiReason?: string,
 * aiEconomyScore?: number,
 * aiEconomyReason?: string,
 * latitude: number,
 * longitude: number
 * }>} */
let stores = [];
/** @type {Record<string, { latitude: number, longitude: number }>} */
let zipCentroids = {};

function toZip5(value) {
  if (value == null) return '';
  const digits = String(value).replace(/[^0-9]/g, '').slice(0, 5);
  return digits.padStart(5, '0');
}

function parseBoolean(value) {
  if (typeof value === 'boolean') return value;
  const s = String(value || '').trim().toLowerCase();
  return s === 'true' || s === '1' || s === 'yes';
}

function haversineMeters(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371000; // meters
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// NYC ZIP code and coordinate helper functions
let nycZipCache = null;

async function loadNYCZips() {
  if (nycZipCache) return nycZipCache;
  
  try {
    const csvPath = path.resolve(__dirname, '..', 'source csv', 'NYC Zip Codes.csv');
    if (fs.existsSync(csvPath)) {
      const results = [];
      return new Promise((resolve, reject) => {
        fs.createReadStream(csvPath)
          .pipe(csv())
          .on('data', (row) => {
            results.push({
              zip: row['ZipCode'],
              borough: row['Borough']
            });
          })
          .on('end', () => {
            nycZipCache = Object.fromEntries(
              results.map(r => [r.zip, r])
            );
            resolve(nycZipCache);
          })
          .on('error', (err) => reject(err));
      });
    }
  } catch (error) {
    console.log('Could not load NYC ZIP codes:', error.message);
  }
  
  return {};
}

function getBoroughCoordinates(borough) {
  // Approximate center coordinates for each NYC borough
  const boroughCoords = {
    'BRONX': { latitude: 40.8448, longitude: -73.8648 },
    'BROOKLYN': { latitude: 40.6782, longitude: -73.9442 },
    'MANHATTAN': { latitude: 40.7831, longitude: -73.9712 },
    'QUEENS': { latitude: 40.7282, longitude: -73.7949 },
    'STATEN ISLAND': { latitude: 40.5795, longitude: -74.1502 }
  };
  
  return boroughCoords[borough.toUpperCase()] || boroughCoords['MANHATTAN'];
}

function getNYCApproximateCoordinates(zip) {
  // For NYC area ZIP codes, provide approximate coordinates
  // This is a fallback when exact coordinates aren't available
  const zipPrefix = zip.substring(0, 3);
  
  // Approximate coordinates based on ZIP code prefixes
  const zipPrefixCoords = {
    '100': { latitude: 40.7589, longitude: -73.9851 }, // Manhattan
    '101': { latitude: 40.7589, longitude: -73.9851 }, // Manhattan (business)
    '102': { latitude: 40.7589, longitude: -73.9851 }, // Manhattan (business)
    '103': { latitude: 40.5795, longitude: -74.1502 }, // Staten Island
    '104': { latitude: 40.8448, longitude: -73.8648 }, // Bronx
    '110': { latitude: 40.7282, longitude: -73.7949 }, // Queens
    '111': { latitude: 40.7282, longitude: -73.7949 }, // Queens
    '112': { latitude: 40.6782, longitude: -73.9442 }, // Brooklyn
    '113': { latitude: 40.7282, longitude: -73.7949 }, // Queens
    '114': { latitude: 40.7282, longitude: -73.7949 }, // Queens
    '116': { latitude: 40.7282, longitude: -73.7949 }  // Queens (Rockaways)
  };
  
  return zipPrefixCoords[zipPrefix] || null;
}

function loadStoresFromCsv(csvPath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(csvPath)
      .pipe(csv())
      .on('data', (row) => {
        const lat = parseFloat(row['Latitude']);
        const lon = parseFloat(row['Longitude']);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return; // skip

        // Parse AI health score if present
        let aiScore = undefined;
        const rawHealthScore = row['AI_Health_Score'];
        if (rawHealthScore !== undefined && rawHealthScore !== null && String(rawHealthScore).trim() !== '') {
          const n = parseInt(String(rawHealthScore), 10);
          if (Number.isFinite(n)) {
            aiScore = Math.max(1, Math.min(10, n));
          }
        }
        const aiReason = String(row['AI_Health_Reason'] || '');

        // Parse AI economy score if present
        let aiEconomyScore = undefined;
        const rawEconomyScore = row['AI_Economy_Score'];
        if (rawEconomyScore !== undefined && rawEconomyScore !== null && String(rawEconomyScore).trim() !== '') {
          const n = parseInt(String(rawEconomyScore), 10);
          if (Number.isFinite(n)) {
            aiEconomyScore = Math.max(1, Math.min(5, n));
          }
        }
        const aiEconomyReason = String(row['AI_Economy_Reason'] || '');

        results.push({
          id: String(row['Record_ID'] || row['ObjectId'] || ''),
          name: String(row['Store_Name'] || ''),
          address: String(row['Store_Street_Address'] || ''),
          city: String(row['City'] || ''),
          borough: String(row['County'] || '').toUpperCase(),
          zip: toZip5(row['Zip_Code']),
          county: String(row['County'] || ''),
          storeType: String(row['Store_Type'] || ''),
          isHealthyStore: parseBoolean(row['Is_Healthy_Store']),
          aiScore,
          aiReason,
          aiEconomyScore,
          aiEconomyReason,
          latitude: lat,
          longitude: lon,
        });
      })
      .on('end', () => resolve(results))
      .on('error', (err) => reject(err));
  });
}

app.get('/health', (_req, res) => {
  res.json({ ok: true, storesLoaded: stores.length });
});

app.get('/stores', (req, res) => {
  const lat = parseFloat(String(req.query.lat || '0'));
  const lng = parseFloat(String(req.query.lng || '0'));
  const radiusMeters = parseFloat(String(req.query.radius || '1609')); // default ~1 mile
  const healthyOnly = String(req.query.isHealthy || 'any').toLowerCase(); // 'true'|'false'|'any'
  const storeType = String(req.query.storeType || '').trim();
  // Parse limit, support 'unlimited' for no cap
  const rawLimit = String(req.query.limit || '200');
  const unlimited = rawLimit.toLowerCase() === 'unlimited';
  let limit = unlimited ? Infinity : parseInt(rawLimit, 10);
  if (!unlimited && (!Number.isFinite(limit) || limit <= 0)) limit = 200;

  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return res.status(400).json({ error: 'lat and lng are required numbers' });
  }

  let filtered = stores;
  if (healthyOnly === 'true') filtered = filtered.filter((s) => s.isHealthyStore);
  else if (healthyOnly === 'false') filtered = filtered.filter((s) => !s.isHealthyStore);

  if (storeType) {
    const typeLc = storeType.toLowerCase();
    filtered = filtered.filter((s) => s.storeType.toLowerCase() === typeLc);
  }

  let withDistance = filtered
    .map((s) => ({
      ...s,
      distance_m: haversineMeters(lat, lng, s.latitude, s.longitude),
    }))
    .filter((s) => s.distance_m <= radiusMeters)
    .sort((a, b) => a.distance_m - b.distance_m)
  if (Number.isFinite(limit)) {
    withDistance = withDistance.slice(0, limit);
  }

  res.json(withDistance);
});

// Full dataset for offline / PWA use — the client computes nearby stores
// locally from this when the network is unavailable.
app.get('/stores/all', (_req, res) => {
  res.set('Cache-Control', 'public, max-age=300');
  res.json(stores);
});

// Enhanced ZIP code lookup with multiple fallback sources
app.get('/zip/:zip', async (req, res) => {
  try {
    const zip = toZip5(req.params.zip);
    if (!zip) return res.status(400).json({ error: 'Invalid ZIP' });
    
    // First try: Use store centroids (most accurate for areas with stores)
    const centroid = zipCentroids[zip];
    if (centroid) {
      return res.json(centroid);
    }
    
    // Second try: Check if it's a valid NYC ZIP code using our comprehensive list
    const nycZipData = await loadNYCZips();
    if (nycZipData[zip]) {
      // Use approximate coordinates based on borough
      const boroughCoords = getBoroughCoordinates(nycZipData[zip].borough);
      return res.json(boroughCoords);
    }
    
    // Third try: Use OpenStreetMap Nominatim API (free, no key required)
    try {
      const nominatimUrl = `https://nominatim.openstreetmap.org/search?postalcode=${zip}&country=US&format=json&limit=1`;
      // Nominatim's usage policy requires an identifying User-Agent or requests are blocked.
      const nominatimRes = await fetch(nominatimUrl, {
        headers: { 'User-Agent': 'SNAPwise-NYC/1.0 (SNAP store locator)' }
      });
      if (nominatimRes.ok) {
        const nominatimData = await nominatimRes.json();
        if (nominatimData.length > 0) {
          const coords = {
            latitude: parseFloat(nominatimData[0].lat),
            longitude: parseFloat(nominatimData[0].lon)
          };
          // Cache this result for future use
          zipCentroids[zip] = coords;
          return res.json(coords);
        }
      }
    } catch (nominatimError) {
      console.log(`Nominatim API failed for ZIP ${zip}:`, nominatimError.message);
    }
    
    // Fourth try: Use USPS ZIP Code API (requires registration but more reliable)
    try {
      const uspsUrl = `https://secure.shippingapis.com/ShippingAPI.dll?API=CityStateLookup&XML=<CityStateLookupRequest><ZipCode ID="0"><Zip5>${zip}</Zip5></ZipCode></CityStateLookupRequest>`;
      const uspsRes = await fetch(uspsUrl);
      if (uspsRes.ok) {
        const uspsText = await uspsRes.text();
        // Parse USPS XML response to check if ZIP is valid
        if (uspsText.includes('<Error>') === false) {
          // ZIP is valid, use approximate NYC coordinates
          const nycCoords = getNYCApproximateCoordinates(zip);
          if (nycCoords) {
            // Cache this result
            zipCentroids[zip] = nycCoords;
            return res.json(nycCoords);
          }
        }
      }
    } catch (uspsError) {
      console.log(`USPS API failed for ZIP ${zip}:`, uspsError.message);
    }
    
    // If all else fails, return error
    return res.status(404).json({ 
      error: 'ZIP not found', 
      message: 'This ZIP code could not be located. Please try a different NYC area ZIP code.' 
    });
    
  } catch (error) {
    console.error('ZIP lookup error:', error);
    return res.status(500).json({ error: 'ZIP lookup failed' });
  }
});

// Rate limiter for /chat — 20 requests per 2-minute window per IP
const chatRateMap = new Map();
const CHAT_RATE_WINDOW_MS = 2 * 60 * 1000;
const CHAT_RATE_MAX = 20;

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of chatRateMap) {
    if (now - entry.windowStart >= CHAT_RATE_WINDOW_MS) chatRateMap.delete(ip);
  }
}, 60_000);

function chatRateLimiter(req, res, next) {
  const ip = req.ip || req.connection.remoteAddress || 'unknown';
  const now = Date.now();
  let entry = chatRateMap.get(ip);

  if (!entry || now - entry.windowStart >= CHAT_RATE_WINDOW_MS) {
    entry = { windowStart: now, count: 0 };
    chatRateMap.set(ip, entry);
  }

  entry.count += 1;

  const remaining = Math.max(0, CHAT_RATE_MAX - entry.count);
  const resetAt = entry.windowStart + CHAT_RATE_WINDOW_MS;
  res.set('X-RateLimit-Limit', String(CHAT_RATE_MAX));
  res.set('X-RateLimit-Remaining', String(remaining));
  res.set('X-RateLimit-Reset', String(Math.ceil(resetAt / 1000)));

  if (entry.count > CHAT_RATE_MAX) {
    const retryAfter = Math.ceil((resetAt - now) / 1000);
    res.set('Retry-After', String(retryAfter));
    return res.status(429).json({
      error: 'Too many requests',
      message: `Limit is ${CHAT_RATE_MAX} coach requests per ${CHAT_RATE_WINDOW_MS / 1000}s. Try again in ${retryAfter}s.`
    });
  }

  next();
}

// Simple Chat proxy for nutrition advice
app.post('/chat', chatRateLimiter, async (req, res) => {
  try {
    
    // Validate request body
    if (!req.body || typeof req.body !== 'object') {
      return res.status(400).json({ error: 'Invalid request body - expected JSON object' });
    }

    if (!OPENAI_API_KEY) {
      return res.status(500).json({ error: 'OpenAI API key not configured' });
    }
    const apiKey = OPENAI_API_KEY;

    const { messages, goal, context, responseLength } = req.body;
    
    // Validate required fields
    if (!messages || !Array.isArray(messages)) {
      return res.status(400).json({ error: 'messages field is required and must be an array' });
    }
    
    if (messages.length === 0) {
      return res.status(400).json({ error: 'messages array cannot be empty' });
    }
    
    // Log the most recent user question
    const userMessages = messages.filter(msg => msg.role === 'user');
    if (userMessages.length > 0) {
      const latestQuestion = userMessages[userMessages.length - 1];
      console.log('User question:', latestQuestion.content);
    }
    
    const length = String(responseLength || 'default');
    let lengthHint = '';
    let maxTokens = 300;
    let temperature = 0.7;
    if (length === 'concise'){
      lengthHint = 'Respond in at most 3 short bullet points (max 15 words each). No intro or outro.';
      maxTokens = 500;
      temperature = 0.4;
    } else if (length === 'comprehensive'){
      lengthHint = 'Provide comprehensive guidance: 6-10 concise bullet points with examples, then a one-sentence summary.';
      maxTokens = 2000;
      temperature = 0.8;
    } else {
      lengthHint = 'Respond in 5-8 succinct sentences with practical steps and examples.';
      maxTokens = 1000;
      temperature = 0.7;
    }
    // Enhanced system prompt with store context awareness
    let systemContent = `You are a supportive nutrition coach for low-income users using SNAP benefits in NYC. Keep responses practical and culturally sensitive. Offer affordable suggestions, highlight whole foods and high-protein budget options, and include quick recipes where relevant. If the user asks for medical advice, include a brief disclaimer and suggest consulting a professional. User goal: ${goal || 'unspecified'}. ${lengthHint}\n\nSTORE LINKING: When mentioning specific store names that exist in the user's area, surround them with %l markers like this: %lStore Name%l. This will automatically make the store name clickable and show it on the map.`;
    
    // Add store-specific guidance if stores are available
    if (context && context.stores && Array.isArray(context.stores) && context.stores.length > 0) {
      systemContent += `\n\nIMPORTANT: You have access to information about ${context.stores.length} stores near the user's location. When the user asks about where to find specific foods, stores, or shopping locations, use this store data to provide specific, actionable recommendations. Include store names, addresses, distances, and any relevant details like store types, health ratings, and price ratings. Make your answers location-specific and practical.\n\nPRICE RATING SCALE: Price ratings range from 1-5 where 1 = most affordable/best value and 5 = most expensive/least value. Use this to help users find budget-friendly options.\n\nSTORE LINKING: When mentioning specific store names that exist in the user's area, surround them with %l markers like this: %lStore Name%l. This will automatically make the store name clickable and show it on the map. For example: "You can find fresh produce at %lLa Mexicana Fruit & Grocery Corp%l which is only 0.3 miles away."`;
    }
    
    const system = {
      role: 'system',
      content: systemContent
    };

    // Enhanced context handling for stores
    let userContext = [];
    if (context) {
      if (context.stores && Array.isArray(context.stores) && context.stores.length > 0) {
        // Format store data for better AI understanding
        const storeInfo = context.stores.map(store => 
          `${store.name} (${store.storeType}) - ${store.address}, ${store.city} ${store.zip} - ${store.distance}m away${store.aiScore ? ` - Health Rating: ${store.aiScore}/10` : ''}${store.aiEconomyScore ? ` - Price Rating: ${store.aiEconomyScore}/5` : ''}`
        ).join('\n');
        
        userContext.push({ 
          role: 'user', 
          content: `Available stores near you:\n${storeInfo}\n\nUse this information to provide specific store recommendations when relevant.` 
        });
      }
      
      if (context.zip) {
        userContext.push({ 
          role: 'user', 
          content: `User's ZIP code: ${context.zip}` 
        });
      }
    }
    const body = {
      model: 'gpt-4o-mini',
      messages: [system, ...userContext, ...(Array.isArray(messages) ? messages : [])].slice(-20),
      temperature,
      max_tokens: maxTokens,
    };

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const text = await response.text();
      console.error('OpenAI error:', response.status, text);
      return res.status(502).json({ error: 'Upstream AI service error' });
    }
    const data = await response.json();
    // console.log('Response Content:', data.choices?.[0]?.message?.content || 'No content');
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Chat proxy failed' });
  }
});

async function start() {
  // Use the actual CSV file location in the server directory
  const csvPath = path.resolve(__dirname, 'NYC Food Stamp Stores.csv');
  
  try {
    if (!fs.existsSync(csvPath)) {
      console.error(`Could not find NYC stores CSV at: ${csvPath}`);
      process.exit(1);
    }
    stores = await loadStoresFromCsv(csvPath);
    console.log(`Loaded ${stores.length} NYC SNAP stores from ${csvPath}.`);
    // Build ZIP centroids from store coordinates
    /** @type {Record<string, { sumLat:number, sumLon:number, count:number }>} */
    const agg = {};
    for (const s of stores) {
      if (!s.zip) continue;
      if (!agg[s.zip]) agg[s.zip] = { sumLat: 0, sumLon: 0, count: 0 };
      agg[s.zip].sumLat += s.latitude;
      agg[s.zip].sumLon += s.longitude;
      agg[s.zip].count += 1;
    }
    zipCentroids = Object.fromEntries(Object.entries(agg).map(([z, v]) => [z, {
      latitude: v.sumLat / v.count,
      longitude: v.sumLon / v.count,
    }]));
    console.log(`Prepared ${Object.keys(zipCentroids).length} ZIP centroids.`);
  } catch (err) {
    console.error('Failed to load stores CSV:', err);
    process.exit(1);
  }

  app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
  });
}

start();


