// =============================================================================
// Reusable JS helpers for SQLX templating.
//
// These functions are imported by SQLX files via:
//   js { const { convertToUsd } = require("includes/helpers"); }
//
// They emit SQL fragments — they do NOT execute SQL. Keep them small and pure.
// =============================================================================

/**
 * Returns a SQL fragment that converts a local-currency amount to USD by
 * looking up the exchange rate snapshot for the given date. Handles NULL rates
 * by returning NULL (never silently substitutes 1.0 — a missing rate is a
 * data quality problem, not a default).
 *
 * @param {string} amountColumn  - SQL expression for the local amount.
 * @param {string} currencyColumn - SQL expression for the ISO currency code.
 * @param {string} dateColumn    - SQL expression for the conversion date.
 * @returns {string} SQL fragment.
 */
function convertToUsd(amountColumn, currencyColumn, dateColumn) {
  return `
    ${amountColumn} * (
      SELECT rate_to_usd
      FROM \`${dataform.projectConfig.defaultDatabase}.raw.currency_exchange_rates\`
      WHERE currency_code = ${currencyColumn}
        AND exchange_date = ${dateColumn}
      LIMIT 1
    )`;
}

/**
 * Returns a SQL CASE expression that classifies a UTC timestamp into a
 * day-part bucket (Morning / Lunch / Afternoon / Evening / Night) based on
 * the local timezone of the row. Used by analytics layers that need
 * comparable behavioral segmentation across timezones.
 *
 * @param {string} timestampColumn - SQL expression for the UTC timestamp.
 * @param {string} timezoneColumn  - SQL expression for the IANA timezone.
 * @returns {string} SQL fragment.
 */
function dayPartLabel(timestampColumn, timezoneColumn) {
  return `
    CASE
      WHEN EXTRACT(HOUR FROM ${timestampColumn} AT TIME ZONE ${timezoneColumn}) BETWEEN 6  AND 10 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM ${timestampColumn} AT TIME ZONE ${timezoneColumn}) BETWEEN 11 AND 14 THEN 'Lunch'
      WHEN EXTRACT(HOUR FROM ${timestampColumn} AT TIME ZONE ${timezoneColumn}) BETWEEN 15 AND 18 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM ${timestampColumn} AT TIME ZONE ${timezoneColumn}) BETWEEN 19 AND 22 THEN 'Evening'
      ELSE 'Night'
    END`;
}

module.exports = {
  convertToUsd,
  dayPartLabel,
};
