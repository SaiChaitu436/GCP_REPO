SELECT *
FROM de-coe.buybox_dataset.src_Offers_Columns AS offers
JOIN de-coe.buybox_dataset.src_Summary_Columns AS summary
ON 
    offers.ASIN = summary.ASIN 
    AND offers.EventTime = summary.EventTime