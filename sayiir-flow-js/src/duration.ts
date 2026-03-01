/**
 * Duration parsing utility.
 *
 * Converts human-readable duration strings (via `ms` library) or numeric
 * milliseconds to milliseconds.
 */

import type { Duration } from "./types.js";
import ms from "ms";

/**
 * Parse a Duration value to milliseconds.
 *
 * - Numbers are returned as-is (assumed to be ms).
 * - Strings are parsed via the `ms` library (e.g. "30s", "5m", "1h").
 */
export function parseDuration(d: Duration): number {
  if (typeof d === "number") return d;
  const result = ms(d as ms.StringValue);
  if (result == null) {
    throw new Error(`Invalid duration: "${d}"`);
  }
  return result;
}
