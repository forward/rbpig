require 'date'
require 'rbpig'

ppr_costs_stats, ppr_conversions_stats = RBPig.connect(File.join(File.dirname(__FILE__), 'hadoop_cluster.xml')) do |pig|
  thirty_days_ago = (Date.today-30).strftime("%Y-%m-%d")
  
  pig.grunt(%[
    ppr_keywords = LOAD '/user/hive/warehouse/ppr_keywords' USING HiveTableLoader;
    ppr_costs_in_last_30_days = FILTER ppr_keywords BY dated >= '#{thirty_days_ago}';
    ppr_costs_grouped = GROUP ppr_costs_in_last_30_days BY dated;
    ppr_costs_stats = FOREACH ppr_costs_grouped GENERATE group AS dated, SUM(ppr_costs_in_last_30_days.cost) AS cost, SUM(ppr_costs_in_last_30_days.impressions) AS impressions, SUM(ppr_costs_in_last_30_days.cost) / SUM(ppr_costs_in_last_30_days.clicks) AS cpc, SUM(ppr_costs_in_last_30_days.clicks) AS clicks, SUM(ppr_costs_in_last_30_days.clicks) / SUM(ppr_costs_in_last_30_days.impressions) * 100 AS ctr, SUM(ppr_costs_in_last_30_days.avg_position) / SUM(ppr_costs_in_last_30_days.impressions) AS avg_pos;

    ppr_conversions = LOAD '/user/hive/warehouse/ppr_conversions' USING HiveTableLoader('\t', 'default');
    ppr_conversions_in_last_30_days = FILTER ppr_conversions BY dated >= '#{thirty_days_ago}';
    ppr_conversions_grouped = GROUP ppr_conversions_in_last_30_days BY dated;
    ppr_conversions_stats = FOREACH ppr_conversions_grouped GENERATE group AS dated, SUM(ppr_conversions_in_last_30_days.conversions) AS conversions;
  ])
  pig.fetch("ppr_costs_stats", "ppr_conversions_stats")
end

[ppr_costs_stats, ppr_conversions_stats].flatten.each do |stat|
  puts stat.inspect
end