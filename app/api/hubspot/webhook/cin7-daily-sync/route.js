export const runtime = "nodejs";

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function isoUtcHoursAgo(hours) {
  const d = new Date(Date.now() - hours * 60 * 60 * 1000);
  return d.toISOString().replace(/\.\d{3}Z$/, "Z"); // trim millis
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

  // Cin7 sometimes returns empty strings; guard it
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
  // Grab available properties so we only write ones that exist in your portal
  const props = await hubspotFetchJson("/crm/v3/properties/orders");
  const names = new Set((props?.results || []).map((p) => p.name));
  return names;
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
  // Use CRM search to find an existing HubSpot order by a unique Cin7 identifier
  // NOTE: this requires that uniquePropName exists as an Order property in HubSpot.
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

export async function GET() {
  const startedAt = new Date().toISOString();

  try {
    const baseUrl = requireEnv("CIN7_BASE_URL").replace(/\/$/, "");
    const path = requireEnv("CIN7_SALES_ORDERS_PATH"); // e.g. "/SalesOrders"
    const lookbackHours = Number(process.env.CIN7_LOOKBACK_HOURS || "48");
    const since = isoUtcHoursAgo(lookbackHours);

    // IMPORTANT: This URL MUST match how your Cin7 Omni endpoint expects filtering.
    // You previously learned InvoiceDate requires UTC datetime like yyyy-MM-ddTHH:mm:ssZ.
    //
    // Replace this querystring with the exact one that worked in Postman.
    const cin7Url = `${baseUrl}${path}?InvoiceDate=${encodeURIComponent(since)}`;

    console.log("[cin7-sync] started", { startedAt, since, cin7Url });

    const cin7Data = await cin7FetchJson(cin7Url);

    // Cin7 APIs vary: some return { items: [...] }, others return an array.
    const salesOrders =
      Array.isArray(cin7Data) ? cin7Data : (cin7Data?.items || cin7Data?.results || []);

    console.log("[cin7-sync] fetched", { count: salesOrders.length });

    // Pull HubSpot Order property names so we only send valid properties
    const hsOrderProps = await getHubSpotOrderPropertyNames();

    // Choose a unique property to store Cin7’s order id/number.
    // BEST PRACTICE: Create a custom Order property in HubSpot called "cin7_order_id" (single-line text, unique).
    // If you do that, set UNIQUE_PROP = "cin7_order_id".
    const UNIQUE_PROP = "cin7_order_id";

    let created = 0;
    let updated = 0;
    let skipped = 0;
    const errors = [];

    for (const o of salesOrders) {
      try {
        // --- Adjust these fields to match what Cin7 returns in your payload ---
        const cin7OrderId =
          String(o?.Id || o?.id || o?.OrderId || o?.SalesOrderId || o?.OrderNumber || "").trim();

        if (!cin7OrderId) {
          skipped++;
          continue;
        }

        const orderTotal =
          o?.Total || o?.OrderTotal || o?.TotalAmount || o?.TotalInclTax || o?.InvoiceTotal;

        const shipping = o?.ShippingAddress || o?.DeliveryAddress || {};
        const shipLine1 = shipping?.Line1 || shipping?.Address1 || shipping?.Street;
        const shipCity = shipping?.City;
        const shipState = shipping?.State || shipping?.Region;
        const shipPostal = shipping?.Postcode || shipping?.Zip;
        const shipCountry = shipping?.Country;

        // Candidate property names (we’ll only keep the ones that exist in your HubSpot portal)
        const candidateProps = {
          [UNIQUE_PROP]: cin7OrderId,

          // Try common Order fields (may vary per portal)
          hs_order_number: o?.OrderNumber || o?.OrderNo || cin7OrderId,
          hs_total_price: orderTotal,
          hs_currency_code: o?.Currency || o?.CurrencyCode || "USD",

          // Shipping fields (may or may not exist in Orders; we only send if they exist)
          ship_to_address: shipLine1,
          ship_to_city: shipCity,
          ship_to_state: shipState,
          ship_to_postal_code: shipPostal,
          ship_to_country: shipCountry,
        };

        const properties = pickExistingProps(hsOrderProps, candidateProps);

        // Ensure the unique property exists before we proceed
        if (!hsOrderProps.has(UNIQUE_PROP)) {
          throw new Error(
            `Missing HubSpot Order property "${UNIQUE_PROP}". Create it in HubSpot (Order properties) as a unique text field, then redeploy.`
          );
        }

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
      fetched: salesOrders.length,
      created,
      updated,
      skipped,
      errors: errors.slice(0, 10),
    });

    return Response.json({
      ok: true,
      startedAt,
      finishedAt,
      since,
      fetched: salesOrders.length,
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
