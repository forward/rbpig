require 'rbpig'
summaries = RBPig.datasets(RBPig::Dataset.hive("ppr_keywords"), RBPig::Dataset.hive("ppr_conversions")) do |pig|
  pig.grunt(%[
    ppr_keywords_in_last_30_days = FILTER ppr_keywords BY dated >= '2010-11-01' AND client == 'ppr';
    ppr_keywords_grouped_by_date = GROUP ppr_keywords_in_last_30_days BY dated;
    ppr_keywords_stats = FOREACH ppr_keywords_grouped_by_date GENERATE group AS dated, SUM(ppr_keywords_in_last_30_days.cost) AS cost, SUM(ppr_keywords_in_last_30_days.impressions) AS impressions, SUM(ppr_keywords_in_last_30_days.cost) / SUM(ppr_keywords_in_last_30_days.clicks) AS cpc, SUM(ppr_keywords_in_last_30_days.clicks) AS clicks, SUM(ppr_keywords_in_last_30_days.clicks) / SUM(ppr_keywords_in_last_30_days.impressions) * 100 AS ctr, SUM(ppr_keywords_in_last_30_days.avg_position) / SUM(ppr_keywords_in_last_30_days.impressions) AS avg_pos;

    ppr_conversions_in_last_30_days = FILTER ppr_conversions BY dated >= '2010-11-01' AND client == 'ppr';
    ppr_conversions_grouped_by_date = GROUP ppr_conversions_in_last_30_days BY dated;
    ppr_conversions_stats = FOREACH ppr_conversions_grouped_by_date GENERATE group AS dated, SUM(ppr_conversions_in_last_30_days.conversions) AS conversions;

    ppr_pl = JOIN ppr_keywords_stats BY dated LEFT OUTER, ppr_conversions_stats BY dated;
    summaries = FOREACH ppr_pl GENERATE 'Summary' AS report_type, ppr_keywords_stats::dated AS record_date, ppr_keywords_stats::impressions AS impressions, ppr_keywords_stats::clicks AS clicks, ppr_keywords_stats::cpc AS cpc, ppr_keywords_stats::ctr AS ctr, ppr_keywords_stats::avg_pos AS avg_pos, ppr_keywords_stats::cost AS cost, ppr_conversions_stats::conversions AS conversions, (ppr_keywords_stats::cost / ppr_conversions_stats::conversions) AS cpa, ((ppr_conversions_stats::conversions / ppr_keywords_stats::clicks) * 100) AS conv_rate;        
  ])
  pig.fetch("summaries")
end

summaries.each do |summary|
  puts summary
end