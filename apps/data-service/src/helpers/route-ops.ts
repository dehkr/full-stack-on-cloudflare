import { getLink } from '@repo/data-ops/queries/links';
import { linkSchema, LinkSchemaType } from '@repo/data-ops/zod-schema/links';
import { LinkClickMessageType } from '@repo/data-ops/zod-schema/queue';
import moment from 'moment';

const TTL_TIME = 60 * 60 * 24; // 1 day

/**
 * Get links from KV store.
 */
async function getLinkInfoFromKv(env: Env, id: string) {
  const parsedLinkInfo = await env.CACHE.get(id, { type: 'json' });
  if (!parsedLinkInfo) {
    return null;
  }
  try {
    return linkSchema.parse(parsedLinkInfo);
  } catch (error) {
    console.error('Link schema validation failed:', error);
    return null;
  }
}

/**
 * Save link info to KV store.
 */
async function saveLinkInfoToKv(env: Env, id: string, linkInfo: LinkSchemaType) {
  try {
    await env.CACHE.put(id, JSON.stringify(linkInfo), {
      expirationTtl: TTL_TIME,
    });
  } catch (error) {
    console.error('Error saving link info to KV:', error);
    throw error;
  }
}

/**
 * Check KV first. If not in KV, retrieve from DB then store in KV.
 */
export async function getRoutingDestinations(env: Env, id: string) {
  const linkInfo = await getLinkInfoFromKv(env, id);
  if (linkInfo) {
    return linkInfo;
  }
  const linkInfoFromDb = await getLink(id);
  if (!linkInfoFromDb) {
    return null;
  }
  await saveLinkInfoToKv(env, id, linkInfoFromDb);

  return linkInfoFromDb;
}

/**
 * Check if country code exists in destinations. If none exists, use default.
 */
export function getDestinationForCountry(linkInfo: LinkSchemaType, countryCode?: string) {
  if (!countryCode) {
    return linkInfo.destinations.default;
  }
  if (linkInfo.destinations[countryCode]) {
    return linkInfo.destinations[countryCode];
  }

  return linkInfo.destinations.default;
}

/**
 * Schedule web page evaluation workflow.
 */
export async function scheduleEvalWorkflow(env: Env, event: LinkClickMessageType) {
  const doId = env.EVALUATION_SCHEDULER.idFromName(
    `${event.data.id}:${event.data.destination}`,
  );
  const stub = env.EVALUATION_SCHEDULER.get(doId);
  await stub.collectLinkClick(
    event.data.accountId,
    event.data.id,
    event.data.destination,
    event.data.country || 'UNKNOWN',
  );
}

/**
 * Capture link click to store in Duroable Object SQL.
 */
export async function captureLinkClickInBackground(
  env: Env,
  event: LinkClickMessageType,
) {
  await env.QUEUE.send(event);

  const doId = env.LINK_CLICK_TRACKER_OBJECT.idFromName(event.data.accountId);
  const stub = env.LINK_CLICK_TRACKER_OBJECT.get(doId);

  if (!event.data.latitude || !event.data.longitude || !event.data.country) {
    return;
  }

  await stub.addClick(
    event.data.latitude,
    event.data.longitude,
    event.data.country,
    moment().valueOf(),
  );
}
