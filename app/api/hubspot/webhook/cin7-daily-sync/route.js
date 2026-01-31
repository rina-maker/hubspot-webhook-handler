import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    ok: true,
    message: "Cin7 daily sync endpoint is live"
  });
}
