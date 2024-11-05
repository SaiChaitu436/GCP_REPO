

  create or replace view `dbt-437212`.`wellbefore`.`dim_summary_values`
  OPTIONS()
  as WITH source_data AS (

    SELECT

        JSON_EXTRACT(MESSAGE_BODY, "$") AS raw_data

    FROM

        dbt-437212.wellbefore.BuyBox_raw_ds_first20_entries

),

flatten_payload AS (

    SELECT

        JSON_EXTRACT_SCALAR(raw_data, "$.EventTime") AS EventTime,

        JSON_EXTRACT_SCALAR(raw_data, "$.NotificationMetadata.NotificationId") AS NotificationId,

        TIMESTAMP(JSON_EXTRACT_SCALAR(raw_data, "$.NotificationMetadata.PublishTime")) AS PublishTime,

        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Offers") AS offers,

        JSON_EXTRACT_SCALAR(raw_data, "$.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN") AS ASIN,

        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.BuyBoxPrices") AS bbp,

        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.LowestPrices") AS lop,

        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.NumberOfBuyBoxEligibleOffers") AS neo,

        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.NumberOfOffers") AS noo,

        JSON_EXTRACT(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.ListPrice") AS lp,

        JSON_EXTRACT_ARRAY(raw_data, "$.Payload.AnyOfferChangedNotification.Summary.SalesRankings") AS sr

    FROM

        source_data

),

flattened_payload AS (

    SELECT

        EventTime,

        NotificationId,

        PublishTime,

        ASIN,

        JSON_EXTRACT_SCALAR(bbp, "$.Condition") AS BuyBoxPricesCondition,

        JSON_EXTRACT_SCALAR(offer, "$.SellerId") AS SellerId,

        CAST(JSON_EXTRACT_SCALAR(bbp, "$.LandedPrice.Amount") AS FLOAT64) AS BuyBoxLandedPriceAmount,

        JSON_EXTRACT_SCALAR(bbp, "$.LandedPrice.CurrencyCode") AS BuyBoxLandedPriceCurrencyCode,

        CAST(JSON_EXTRACT_SCALAR(bbp, "$.ListingPrice.Amount") AS FLOAT64) AS BuyBoxListingPriceAmount,

        JSON_EXTRACT_SCALAR(bbp, "$.ListingPrice.CurrencyCode") AS BuyBoxListingPriceCurrencyCode,

        CAST(JSON_EXTRACT_SCALAR(bbp, "$.Shipping.Amount") AS FLOAT64) AS BuyBoxShippingAmount,

        JSON_EXTRACT_SCALAR(bbp, "$.Shipping.CurrencyCode") AS BuyBoxShippingCurrencyCode,

        JSON_EXTRACT_SCALAR(lop, "$.Condition") AS LowestPricesCondition,

        JSON_EXTRACT_SCALAR(lop, "$.FulfillmentChannel") AS LowestPricesFulfillmentChannel,

        CAST(JSON_EXTRACT_SCALAR(lop, "$.LandedPrice.Amount") AS FLOAT64) AS LandedPriceAmount,

        JSON_EXTRACT_SCALAR(lop, "$.LandedPrice.CurrencyCode") AS LandedPriceCurrencyCode,

        CAST(JSON_EXTRACT_SCALAR(lop, "$.ListingPrice.Amount") AS FLOAT64) AS LowestPricesListingPriceAmount,

        JSON_EXTRACT_SCALAR(lop, "$.ListingPrice.CurrencyCode") AS LowestPricesListingCurrencyCode,

        CAST(JSON_EXTRACT_SCALAR(lop, "$.Shipping.Amount") AS FLOAT64) AS LowestPricesShippingPriceAmount,

        JSON_EXTRACT_SCALAR(lop, "$.Shipping.CurrencyCode") AS LowestPricesShippingCurrencyCode,

        JSON_EXTRACT_SCALAR(neo, "$.Condition") AS BuyBoxEligibleOffersCondition,

        JSON_EXTRACT_SCALAR(neo, "$.FulfillmentChannel") AS BuyBoxEligibleOffersFulfillmentChannel,

        JSON_EXTRACT_SCALAR(neo, "$.OfferCount") AS BuyBoxEligibleOffersCount,

        JSON_EXTRACT_SCALAR(noo, "$.Condition") AS NumberOfOffersCondition,

        JSON_EXTRACT_SCALAR(noo, "$.FulfillmentChannel") AS NumberOfOffersFulfillmentChannel,

        JSON_EXTRACT_SCALAR(noo, "$.OfferCount") AS NumberOfOffersCount,

        JSON_EXTRACT_SCALAR(sr, "$.ProductCategoryId") AS SalesRankingsProductCategoryId,

        CAST(JSON_EXTRACT_SCALAR(sr, "$.Rank") AS INT64) AS SalesRankingsRank,

        CAST(JSON_EXTRACT_SCALAR(lp, "$.Amount") AS FLOAT64) AS ListPriceAmount,

        JSON_EXTRACT_SCALAR(lp, "$.CurrencyCode") AS ListPriceCurrencyCode

    FROM

        flatten_payload,

        UNNEST(offers) AS offer,

        UNNEST(bbp) AS bbp,

        UNNEST(lop) AS lop,

        UNNEST(neo) AS neo,

        UNNEST(noo) AS noo,

        UNNEST(sr) AS sr

)

SELECT *

FROM flattened_payload order by ASIN;

