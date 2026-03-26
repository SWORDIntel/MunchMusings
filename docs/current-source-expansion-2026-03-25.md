# Current Source Expansion

Generated on 2026-03-25.

## What changed

The seeded source stack was expanded from 20 to 34 sources to improve current-source collection quality in five weak areas:
- macro-price and inflation confounds
- humanitarian update discovery
- market-monitor feeds that sit between baselines and local place collection
- regional shipping chokepoints and maritime throughput
- Israel official trade, price, and logistics monitoring

## Verified additions

1. `FAO FPMA Food Price Monitoring and Analysis`
   - URL: https://www.fao.org/giews/food-prices/en/
   - Why it was added: FAO positions FPMA as a timely and reliable food-price analysis tool with a monthly bulletin, making it a strong macro confound layer.

2. `Saudi GASTAT Consumer Price Index`
   - URL: https://stats.gov.sa/en/w/cpi-1
   - Why it was added: Saudi GASTAT published a CPI release on 2026-01-15 covering December 2025 inflation, which makes it a current official macro-price source.

3. `Lebanon CAS Consumer Price Index`
   - URL: https://cas.gov.lb/index.php/economic-statistics-en/cpi-en
   - Why it was added: The CAS CPI area remains the official Lebanon inflation reference and exposes current inflation indicators needed for confound control.

4. `HDX Humanitarian API`
   - URL: https://data.humdata.org/hapi
   - Why it was added: OCHA's April 2025 HDX one-pager describes HAPI as the standardised humanitarian API for workflow automation.

5. `HDX Signals`
   - URL: https://data.humdata.org/signals
   - Why it was added: The same April 2025 HDX one-pager describes Signals as an automated change-detection product for key datasets, which directly improves source refresh discovery.

6. `UNHCR Egypt Sudan Emergency Update`
   - URL: https://reporting.unhcr.org/egypt-sudan-emergency-update
   - Why it was added: The UNHCR reporting stream exposes Egypt-Sudan emergency update pages beyond the broader Egypt portal, tightening displacement baseline refresh.

7. `WFP Global Market Monitor`
   - URL: https://data.humdata.org/dataset/global-market-monitor
   - Why it was added: HDX describes this as a current WFP market-monitor dataset with recurring updates, making it a stronger market-monitor seed than older static country dashboard pages.

8. `UN Comtrade TradeFlow`
   - URL: https://comtradeplus.un.org/TradeFlow/
   - Why it was added: It is the strongest machine-friendly multilateral trade backbone for tracking commodity and merchandise flows across the corridor states.

9. `UNCTAD Maritime Transport Insights`
   - URL: https://unctadstat.unctad.org/insights/theme/246
   - Why it was added: It adds maritime transport and port-profile context that can explain route stress before local retail signals move.

10. `Suez Canal Authority Navigation News`
   - URL: https://www.suezcanal.gov.eg/English/MediaCenter/News/Pages/Sca_3-3-2026.aspx
   - Why it was added: It provides a chokepoint indicator for Red Sea and Eastern Mediterranean shipping conditions that affect regional supply chains.

11. `Israel CBS Main Price Indices`
   - URL: https://www.cbs.gov.il/en/Pages/Main%20Price%20Indices.aspx
   - Why it was added: It is the cleanest official Israel macro-price baseline for controlling inflation and food-price shifts.

12. `Israel CBS Exports and Imports Monthly Files`
   - URL: https://www.cbs.gov.il/en/Pages/importAndExport.aspx
   - Why it was added: It adds monthly official Israel commodity and country trade flows needed for supply-shock and corridor analysis.

13. `Ashdod Port Financial and Operating Updates`
   - URL: https://www.ashdodport.co.il/about/financial-information/Pages/default.aspx
   - Why it was added: It adds a port-activity and throughput source for one of Israel's key maritime gateways.

14. `Israel Airports Authority Monthly Report`
   - URL: https://www.iaa.gov.il/about/aeronautical-information/annualreport/
   - Why it was added: It provides a monthly air-cargo comparator when maritime routes are disrupted or rerouted.

## Pipeline impact

- `docs/source-registry.csv` now carries 34 sources.
- `plans/recent_accounting.csv` now tracks the new macro-price, humanitarian-feed, and market-monitor seeds.
- `artifacts/collection/source-adapter-registry.csv` and `artifacts/collection/collection-run-manifest.csv` now include runnable rows for the new sources.
- `artifacts/briefings/cairo-giza-pilot-2026-03-25/aggregated_signals.csv` now exists as the fused layer between observations and anomaly cards.

## Remaining gap

The new sources are seeded and scheduled, but most of them are still `unknown` in `plans/recent_accounting.csv` until their latest publication dates are verified and captured into the ledger.
