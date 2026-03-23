/**
 * Standalone refresh worker - runs track scraping without Next.js.
 * Called by refresh-data.js via tsx.
 */

import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

// --- Inline scraping functions (no 'server-only' import needed) ---

import * as cheerio from 'cheerio';

const FETCH_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
};

interface TrackRaw {
  trackName: string;
  artistName: string;
  rank: number;
  dailyStreams: number;
  totalStreams?: number;
}

function parseNumber(text: string): number {
  if (!text) return 0;
  let cleaned = text.replace(/[, ]/g, '');
  let multiplier = 1;
  if (cleaned.endsWith('M')) { multiplier = 1000000; cleaned = cleaned.replace('M', ''); }
  else if (cleaned.endsWith('K')) { multiplier = 1000; cleaned = cleaned.replace('K', ''); }
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : Math.round(num * multiplier);
}

async function scrapeKworbDailyTracks(countryCode: string): Promise<TrackRaw[]> {
  const url = `https://kworb.net/spotify/country/${countryCode}_daily.html`;
  const response = await fetch(url, { headers: FETCH_HEADERS });
  if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);

  const html = await response.text();
  const $ = cheerio.load(html);
  const tracks: TrackRaw[] = [];
  const seenRanks = new Set<number>();

  $('table tr').each((index, element) => {
    if (index === 0) return;
    const cells = $(element).find('td');
    if (cells.length < 7) return;

    const rank = parseInt($(cells[0]).text().trim().replace(/[^\d]/g, ''), 10);
    if (isNaN(rank) || rank < 1 || rank > 100) return;

    const dailyStreams = parseNumber($(cells[6]).text().trim());
    if (dailyStreams < 10000) return;

    const artistTitleText = $(cells[2]).text().trim();
    if (!artistTitleText || /^[\d\s\-=]+$/.test(artistTitleText) || artistTitleText.length < 3) return;

    const parts = artistTitleText.split(' - ');
    let trackName = '', artistName = '';
    if (parts.length >= 2) {
      artistName = parts[0].trim();
      trackName = parts[1].trim();
    } else {
      trackName = artistTitleText;
      artistName = 'Unknown';
    }

    if (!/[a-zA-Z]/.test(trackName) || !/[a-zA-Z]/.test(artistName)) return;
    if (!trackName || trackName.length < 2 || /^[=\+\-\s]+$/.test(trackName)) return;
    if (!artistName || artistName.length < 2 || /^[\d\s\-=]+$/.test(artistName)) return;
    if (seenRanks.has(rank)) return;
    seenRanks.add(rank);

    let totalStreams: number | undefined;
    if (cells.length >= 11) {
      totalStreams = parseNumber($(cells[10]).text().trim()) || undefined;
    }

    tracks.push({ trackName, artistName, rank, dailyStreams, totalStreams });
  });

  tracks.sort((a, b) => a.rank - b.rank);
  const limit = parseInt(process.env.TOP_TRACKS_LIMIT || '25', 10);
  return tracks.slice(0, limit);
}

// --- Spotify metadata resolution ---

async function getSpotifyToken(): Promise<string | null> {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  if (!clientId || !clientSecret) return null;

  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': 'Basic ' + Buffer.from(`${clientId}:${clientSecret}`).toString('base64'),
    },
    body: 'grant_type=client_credentials',
  });

  if (!res.ok) return null;
  const data = await res.json();
  return data.access_token;
}

interface TrackMeta {
  spotifyId: string;
  imageUrl?: string;
  previewUrl?: string;
  url?: string;
}

async function resolveTrack(trackName: string, artistName: string, token: string): Promise<TrackMeta | null> {
  const query = encodeURIComponent(`track:${trackName} artist:${artistName}`);
  const res = await fetch(`https://api.spotify.com/v1/search?q=${query}&type=track&limit=1`, {
    headers: { 'Authorization': `Bearer ${token}` },
  });
  if (!res.ok) return null;
  const data = await res.json();
  const track = data.tracks?.items?.[0];
  if (!track) return null;
  return {
    spotifyId: track.id,
    imageUrl: track.album?.images?.[0]?.url,
    previewUrl: track.preview_url,
    url: track.external_urls?.spotify,
  };
}

// --- Countries config ---

function getCountriesToScrape(): string[] {
  const env = process.env.SCRAPE_COUNTRIES;
  if (env) return env.split(',').map(c => c.trim().toLowerCase()).filter(Boolean);
  return ['global', 'id'];
}

// --- Main refresh logic ---

async function refreshAllStats() {
  const countries = getCountriesToScrape();
  console.log(`Scraping ${countries.length} countries: ${countries.join(', ')}`);

  const spotifyToken = await getSpotifyToken();
  if (!spotifyToken) {
    console.warn('⚠️  No Spotify token available, tracks will not be enriched with metadata');
  }

  for (const country of countries) {
    console.log(`\n--- Scraping ${country} tracks ---`);
    
    try {
      const tracks = await scrapeKworbDailyTracks(country);
      console.log(`Scraped ${tracks.length} tracks`);

      // Store snapshots
      await prisma.trackSnapshot.createMany({
        data: tracks.map(t => ({
          trackName: t.trackName,
          artistName: t.artistName,
          country,
          rank: t.rank,
          dailyStreams: BigInt(t.dailyStreams),
          totalStreams: t.totalStreams ? BigInt(t.totalStreams) : null,
        })),
      });

      // Process each track
      const startTime = new Date();
      for (const track of tracks) {
        // Get daily baseline
        const todayStart = new Date();
        todayStart.setHours(0, 0, 0, 0);
        const baseline = await prisma.trackSnapshot.findFirst({
          where: { trackName: track.trackName, artistName: track.artistName, country, createdAt: { lt: todayStart } },
          orderBy: { createdAt: 'desc' },
        });
        const previousRank = baseline?.rank ?? null;
        const rankDelta = previousRank !== null ? track.rank - previousRank : null;

        // Check existing metadata
        const existing = await prisma.trackCurrent.findUnique({
          where: { trackName_artistName_country: { trackName: track.trackName, artistName: track.artistName, country } },
        });

        let trackId = existing?.trackId ?? null;
        let imageUrl = existing?.imageUrl ?? null;
        let previewUrl = existing?.previewUrl ?? null;
        let spotifyUrl = existing?.spotifyUrl ?? null;

        // Enrich if needed
        if (!trackId && spotifyToken) {
          console.log(`  Enriching: ${track.trackName} by ${track.artistName}`);
          const meta = await resolveTrack(track.trackName, track.artistName, spotifyToken);
          if (meta) {
            trackId = meta.spotifyId;
            imageUrl = meta.imageUrl ?? null;
            previewUrl = meta.previewUrl ?? null;
            spotifyUrl = meta.url ?? null;
          }
          await new Promise(r => setTimeout(r, 100));
        }

        // Upsert
        await prisma.trackCurrent.upsert({
          where: { trackName_artistName_country: { trackName: track.trackName, artistName: track.artistName, country } },
          update: {
            rank: track.rank, previousRank, rankDelta,
            dailyStreams: BigInt(track.dailyStreams),
            totalStreams: track.totalStreams ? BigInt(track.totalStreams) : null,
            trackId: trackId ?? undefined, imageUrl: imageUrl ?? undefined,
            previewUrl, spotifyUrl: spotifyUrl ?? undefined,
            lastUpdated: new Date(),
          },
          create: {
            trackName: track.trackName, artistName: track.artistName, country,
            rank: track.rank, previousRank, rankDelta,
            dailyStreams: BigInt(track.dailyStreams),
            totalStreams: track.totalStreams ? BigInt(track.totalStreams) : null,
            trackId, imageUrl, previewUrl: previewUrl ?? null, spotifyUrl,
          },
        });
      }

      // Cleanup stale
      const deleted = await prisma.trackCurrent.deleteMany({
        where: { country, lastUpdated: { lt: startTime } },
      });
      if (deleted.count > 0) console.log(`  Cleaned up ${deleted.count} stale tracks`);

      console.log(`✅ ${country} done`);
    } catch (error) {
      console.error(`❌ Error scraping ${country}:`, error);
    }
  }
}

// Run
refreshAllStats()
  .then(() => {
    console.log('\n🎉 All done!');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
