# Team FDA Recalls

## Team members
Grace Pitts and Alka Link

## Data Source
What data source did you work with?

FDA recall data (drug and food recall)

## Challenges / Obstacles
What challenges did this data choice present in data gathering, processing and analysis, and how did you work through them? What methods and tools did you use to work with this data?

This data choice presented us with issues in data gathering because there were not as many data entries as we had hoped. We had originally planned to just work with FDA drug recall data, although after ingesting the data, we found that there were only 17k records. We knew we needed more data, so we decided to also add FDA food recall data which got us almost to 50k entries, giving us way more to work with. Another problem we encountered with data gathering is that the API limits how much data you can skip so we couldn't download everything at once. There was some missing data, duplicates, and sometimes blocked requests which did not give us the easiest dataset to work with. In order to fix these problems, we changed how we downloaded and stored the data. We used smaller time ranges, filled in missing fields if possible, removed duplicates, and added a retry logic if we ran into an error.  For tools, we used an API in order to get the data that we used from the FDA website. We also used DuckDB to help with data storage. We used a Docker container to avoid further software issues. 


## Analysis
Offer a brief analysis of the data with your findings. Keep it to one brief, clear, and meaningful paragraph.

**Brief Analysis of FDA Recall Trends (2012-2025)**

FDA recall activity shows long term structural patterns that reveal where product risk is most heavily concentrated and how regulatory pressures have evolved. Across food and drug recalls it is clear that manufacturing and sterility failures, not small contamination events, drive the majority of recalls. 

Key Insights:

- Sterility and cGMP failures dominate recalls, showing the systemic manufacturing issues. The most common recall reason by a wide margin is “Lack of Assurance of Sterility,” followed by many cGMP (current good manufacturing practice) failures. This suggests that recalls are a lot less about unexpected hazards and more about quality system failures, like improper documentation, temperature excursions, inadequate validation processes, etc. The real world implications of this is that these issues scale across entire production lots, which can increase regulatory burden. They are also preventable with better process control, which means that there could be a strong ROI for automated monitoring technology. 
- Food recalls consistently outpace drug recalls, but the gap is narrowing in recent years. From 2012-2017, food recalls made up the majority of all recalls, but after 2018, food’s share declines steadily while drugs steadily rise. In addition to this, drug recalls have stayed relatively stable, while food recalls have sharp year-to-year spikes, which can be caused by supply chain shocks, outbreaks, or increased surveillance. 
- Recall frequency is seasonal in food but not in drugs. Monthly trends show that food recalls spike every spring/summer. This aligns with temperature related spoiler risks, peak agricultural distribution periods, and seasonal bacteria proliferation. Drugs lack this seasonality, which further supports that their recalls come from process failures and not environmental factors.
- Long term cumulative recalls rise steadily in both food and drugs, which suggests increased FDA oversight rather than declining product safety. These steady, linear curves can mean that there are more intense surveillance programs, better reporting infrastructure, and greater transparency rather than greater failures. This shows that recall counts alone cannot measure safety trends and it is important to take a closer look at the data. 
- Class II dominates the recalls, meaning that most hazards are moderate but widespread. Class II (medium risk) occurs twice as often as Class I (high risk), showing that most recalls are not about imminent danger but about compliance lapses that still have meaningful public health risks.


## Plot / Visualization

Include at least one compelling plot or visualization of your work. Add images in your subdirectory and then display them using markdown in your README.md file.

## GitHub Repository
https://github.com/gracepitts/fda-recalls 