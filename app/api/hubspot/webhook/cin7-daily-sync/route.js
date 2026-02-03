export const runtime = "nodejs";

/**
 * Cin7 → HubSpot Orders daily sync (idempotent without batch upsert)
 *
 * Env vars required:
 *  - HUBSPOT_PRIVATE_APP_TOKEN
 *  - CIN7_USERNAME
 *  - CIN7_KEY
 *  - CIN7_BASE_URL              e.g. https://api.cin7.com/api/v1
 *  - CIN7_SALES_ORDERS_PATH     e.g. /SalesOrders
 *
 * Optional:
 *  - CIN7_LOOKBACK_HOURS        default 48
 *  - CIN7_FORCE_SINCE           ISO UTC datetime e.g. 2026-01-21T00:00:00Z
 *  - CIN7_ROWS                  default 250
 *  - CIN7_MAX_PAGES             default 50
 */

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function isoUtcHoursAgo(hours) {
  const d = new Date(Date.now() - hours * 60 * 60 * 1000);
  // Trim millis so it matches Cin7 filter expectations
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function cin7FetchJson(url) {
  const username = requireEnv("CIN7_USERNAME");
  const key = requireEnv("CIN7_KEY");
  const auth = Buffer.from(`${username}:${key}`).toString("base64");

  const resp = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Basic ${auth}`,
      Accept: "application/json",
    },
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Cin7 error ${resp.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function hubspotFetchJson(path, { method = "GET", body } = {}) {
  const token = requireEnv("HUBSPOT_PRIVATE_APP_TOKEN");

  const resp = await fetch(`https://api.hubapi.com${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`HubSpot error ${resp.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function getHubSpotOrderPropertyNames() {
  const props = await hubspotFetchJson("/crm/v3/properties/orders");
  return new Set((props?.results || []).map((p) => p.name));
}

function pickExistingProps(allNamesSet, candidateProps) {
  const out = {};
  for (const [k, v] of Object.entries(candidateProps)) {
    if (v === undefined || v === null || v === "") continue;
    if (allNamesSet.has(k)) out[k] = v;
  }
  return out;
}

async function findOrderByUniqueProperty(uniquePropName, uniqueValue) {
  const body = {
    filterGroups: [
      {
        filters: [
          {
            propertyName: uniquePropName,
            operator: "EQ",
            value: uniqueValue,
          },
        ],
      },
    ],
    properties: [uniquePropName],
    limit: 1,
  };

  const res = await hubspotFetchJson("/crm/v3/objects/orders/search", {
    method: "POST",
    body,
  });

  const found = res?.results?.[0];
  return found?.id || null;
}

async function createOrder(properties) {
  return hubspotFetchJson("/crm/v3/objects/orders", {
    method: "POST",
    body: { properties },
  });
}

async function updateOrder(orderId, properties) {
  return hubspotFetchJson(`/crm/v3/objects/orders/${orderId}`, {
    method: "PATCH",
    body: { properties },
  });
}

function normalizeCin7Id(o) {
  return String(
    o?.id ??
      o?.Id ??
      o?.OrderId ??
      o?.SalesOrderId ??
      o?.OrderNumber ??
      o?.reference ??
      ""
  ).trim();
}

function normalizeTotal(o) {
  return (
    o?.total ??
    o?.Total ??
    o?.orderTotal ??
    o?.OrderTotal ??
    o?.totalAmount ??
    o?.TotalAmount ??
    o?.productTotal ??
    o?.ProductTotal ??
    null
  );
}

export async function GET() {
  const startedAt = new Date().toISOString();

  try {
    // --- Cin7 env & time window ---
    const baseUrl = requireEnv("CIN7_BASE_URL").replace(/\/$/, "");
    const path = requireEnv("CIN7_SALES_ORDERS_PATH"); // e.g. "/SalesOrders"

    const lookbackHours = Number(process.env.CIN7_LOOKBACK_HOURS || "48");
    const forcedSinceRaw = process.env.CIN7_FORCE_SINCE;
    const forcedSince = forcedSinceRaw && forcedSinceRaw.trim() ? forcedSinceRaw.trim() : null;

    const since = forcedSince || isoUtcHoursAgo(lookbackHours);

    // pagination controls
    const rows = Number(process.env.CIN7_ROWS || "250");
    const maxPages = Number(process.env.CIN7_MAX_PAGES || "50");

    console.log("[cin7-sync] started", {
      startedAt,
      since,
      rows,
      maxPages,
      forcedSince: Boolean(forcedSince),
    });

    // --- HubSpot: confirm the unique property exists ---
    const hsOrderProps = await getHubSpotOrderPropertyNames();
  console.log("[cin7-sync] sample order props available", 
  Array.from(hsOrderProps).filter(n =>
    n.includes("ship") || n.includes("address") || n.includes("city") ||
    n.includes("state") || n.includes("postal") || n.includes("country") ||
    n.includes("name") || n.includes("total") || n.includes("currency")
  ).slice(0, 200)
);
    // IMPORTANT: this must match the INTERNAL name in HubSpot Order properties
    const UNIQUE_PROP = "cin7_order_id";

    if (!hsOrderProps.has(UNIQUE_PROP)) {
      throw new Error(
        `Missing HubSpot Order property "${UNIQUE_PROP}". Create it in HubSpot (Orders properties) as single-line text (unique), then redeploy.`
      );
    }

    // --- Build Cin7 filter (match what worked in Postman) ---
    // InvoiceDate must be UTC ISO like 2026-01-21T00:00:00Z
    const where = `invoiceDate >= '${since}' AND invoiceDate IS NOT NULL`;

    // Pull the Cin7 fields you actually need
    const fields =
      "id,reference,invoiceDate,stage," +
      "deliveryFirstName,deliveryLastName,deliveryCompany," +
      "deliveryAddress1,deliveryAddress2,deliveryCity,deliveryState,deliveryPostalCode,deliveryCountry," +
      "freightTotal,productTotal,total";

    let page = 1;
    const allOrders = [];

    while (true) {
      const cin7Url =
        `${baseUrl}${path}` +
        `?where=${encodeURIComponent(where)}` +
        `&fields=${encodeURIComponent(fields)}` +
        `&order=invoiceDate` +
        `&rows=${rows}` +
        `&page=${page}`;

      console.log("[cin7-sync] fetching page", { page });

      const cin7Data = await cin7FetchJson(cin7Url);
      const orders = Array.isArray(cin7Data)
        ? cin7Data
        : (cin7Data?.items || cin7Data?.results || []);

      if (!orders.length) break;

      allOrders.push(...orders);
      page += 1;

      if (page > maxPages) break;
    }

    console.log("[cin7-sync] fetched from cin7", { count: allOrders.length });

    // --- Prepare upserts (dedupe by cin7 id) ---
    const seen = new Set();
    const prepared = [];
    let skipped = 0;

    for (const o of allOrders) {
      const cin7OrderId = normalizeCin7Id(o);
      if (!cin7OrderId) {
        skipped++;
        continue;
      }
      if (seen.has(cin7OrderId)) continue;
      seen.add(cin7OrderId);
      prepared.push(o);
    }

    console.log("[cin7-sync] prepared inputs", { prepared: prepared.length, skipped });

    // --- Upsert: search -> create/update ---
    let created = 0;
    let updated = 0;
    const errors = [];

    for (const o of prepared) {
      try {
        const cin7OrderId = normalizeCin7Id(o);
        const orderTotal = normalizeTotal(o);
        
const shippingName =
  o?.deliveryCompany ||
  `${o?.deliveryFirstName || ""} ${o?.deliveryLastName || ""}`.trim();

const street =
  o?.deliveryAddress1 ||
  o?.deliveryAddress2 ||
  "";

const candidateProps = {
  // Idempotency key (custom unique property)
  [UNIQUE_PROP]: cin7OrderId,

  // Order identity
  hs_order_name: `Cin7 Order ${o?.reference || cin7OrderId}`,
  hs_currency_code: "USD",

  // Totals
  hs_subtotal_price: o?.productTotal,
  hs_shipping_cost: o?.freightTotal,
  hs_total_price: o?.total,

  // Shipping
  hs_shipping_address_name: shippingName,
  hs_shipping_address_street: street,
  hs_shipping_address_city: o?.deliveryCity,
  hs_shipping_address_state: o?.deliveryState,
  hs_shipping_address_postal_code: o?.deliveryPostalCode,
  hs_shipping_address_country: o?.deliveryCountry,
    // ✅ Cin7 → HubSpot custom mapping
  cin7_company: o?.billingCompany,
};
        const properties = pickExistingProps(hsOrderProps, candidateProps);

        const existingId = await findOrderByUniqueProperty(UNIQUE_PROP, cin7OrderId);

        if (existingId) {
          await updateOrder(existingId, properties);
          updated++;
        } else {
          await createOrder(properties);
          created++;
        }
      } catch (e) {
        errors.push(String(e?.message || e));
      }
    }

    const finishedAt = new Date().toISOString();

    console.log("[cin7-sync] complete", {
      startedAt,
      finishedAt,
      since,
      cin7Count: allOrders.length,
      prepared: prepared.length,
      created,
      updated,
      skipped,
      errorsCount: errors.length,
      sampleErrors: errors.slice(0, 5),
    });

    return Response.json({
      ok: true,
      startedAt,
      finishedAt,
      since,
      cin7Count: allOrders.length,
      prepared: prepared.length,
      created,
      updated,
      skipped,
      errorsCount: errors.length,
      sampleErrors: errors.slice(0, 3),
    });
  } catch (err) {
    console.error("[cin7-sync] failed", err);
    return Response.json(
      { ok: false, error: String(err?.message || err) },
      { status: 500 }
    );
  }
}
