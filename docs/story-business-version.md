V0: 

# 1: There are 1M+ retail locations in the US
# 2: When you get your network right, you unlock millions in new revenue. Get it wrong, and you're stuck with upto $5M mistake that takes years to unwind. Therefore, every single location in your network is a $5 Million dollar decision
# 3: Naturally, the question becomes, "where should I build next", and subsequently "Is this the right spot"
# 4: Challenges Today: 
# 4.1: Speed Problem: Best case, depending on the size of the network, a month at least
# 4.2: The Platform Problem
# 4.3: The Scale Problem
# 4.4: The last mile is a game of telephone between your Data Scientists and Business Folks making the decision
# 5: Here's how the this would look like - Insert Current Architecture
# 6: Well what if it did not have to look like this? : Insert Databricks Architecture
# 7: Let's look at how this solution would look like

V1: 

# Slide Script

## Slide 1: Hero (B01-Hero) - //comment Should be the center of the page. 
- **Title:** Where Should You Build Next?
- **Subtitle:** Site Selection & Network Optimization on Databricks
- **Footer:** Databricks | March 2026

## Slide 2: Stat — Locations (B02-StatLocations)
- **Big number:** 1M+
- **Primary text:** Retail chain locations in the United States
- **Secondary text:** Every new site is a multi-million dollar bet on geography.

## Slide 3: Stat — Cost (B03-StatCost)
- **Lead-in:** When you get your network right, you unlock millions in new revenue.
- **Transition:** Get it wrong, and you're stuck with up to a
- **Big number:** $5M
- **Continuation:** mistake that takes years to unwind.
// comment : remove the line below, I will voice this over
- **Callout badge:** Every single location in your network could be a $5 Million dollar decision.

## Slide 4: The Question (B04-TheQuestion)
- **Primary question:** "Where should I build next?"
- **Secondary question:** "Is this the right spot?"

// comment: Insert translatory slide: "So let us look at the challenges behind making this $5 Million decison"


// I want all the challenges to be in one page: They should appear one by one left to right in cards (center aligned) When the speed problem card pops up and i hit the -> arrow, it should show what does a speed problem mean i.e, Lots of stake holders involved, needs to be vetted by business, and the analysis timeline could be anywhere from a month to a few months. Use this logic to summarize the rest of the challenges in one page. 
## Slide 5: The Speed Problem (B05-ChallengeSpeed)
- **Title:** The Speed Problem
- **Subtitle:** Best case: a month. Full cycle: 6-18 months. By the time your analysis is done, the market has moved.
- **Bar chart phases:**
  - Data Collection — 1-2 weeks
  - Standardization & Joins — 2 weeks
  - Feature Engineering — 1-2 weeks
  - Model Training & Scoring — 2 weeks
  - Business Review & Iteration — 1-4 weeks
  - Final Report / Delivery — 2-5 weeks


## Slide 6: The Platform Problem (B06-ChallengePlatform)
- **Title:** The Platform Problem
- **Subtitle:** You try to get everything into one place — but every option has trade-offs.
- **Column 1 — Use a GIS platform?**
  - Manually import all other data sources
  - Extra licensing fees to get data back out
  - Locked into one vendor's ecosystem
- **Column 2 — Use custom scripts?**
  - Export from GIS manually
  - Build custom infrastructure for every operation
  - Maintain it all yourself
- **Column 3 — Use a BI tool?**
  - Not built for geographic analysis
  - No drive-time calculations
  - No spatial overlap or trade area modeling
- **Bottom badge:** Either way: months of manual work — and your data still isn't unified

## Slide 7: The Scale Problem (B07-ChallengeScale)
- **Title:** The Scale Problem
- **Subtitle:** The more locations you have, the exponentially longer it takes. Every assumption change restarts the clock.
- **Bar chart rows:**
  - 50 stores — Minutes
  - 200 stores — Hours
  - 500 stores — Days
  - 1,000+ stores — Weeks
- **Bottom note:** Score thousands of candidate sites, optimize for cannibalization, layer in constraints — and then do it all again when assumptions change.

## Slide 8: The Last Mile Problem (B08-ChallengeLastMile)
- **Title:** The Last Mile Problem
- **Subtitle:** A game of telephone between your Data Scientists and the Business folks making the decision.
- **Chat messages:**
  - **Data Team:** We identified 400 optimal locations.
  - **Business:** That's not enough. We need at least 800.
  - **Data Team:** Relaxing constraints... re-running the model. (2 weeks later)
  - **Business:** Can we see what happens if we close the underperformers first?
  - **Data Team:** That's a different analysis. Give us another month.
- **Bottom text:** Sound familiar?

// I like this, but I want it to look a little more complex. Back and forths are missing, the data types are different, lack of ML flow. There also a gross overestimation of the timeline. I have personally done this in 1 to 1.5 months (10-12 weeks - but again I worked overtime, so maybe say 2-3 months)
## Slide 9: Current Architecture (B09-CurrentArch)
- **Title:** Here's What That Looks Like Today
- **Subtitle:** The end-to-end site selection pipeline most teams are running.
- **Column headers:** DATA SOURCES | PROCESSING | OUTPUT
- **Data Sources:**
  - Demographics — Census Bureau / ESRI
  - Foot Traffic — Placer.ai / SafeGraph
  - Competition — OSM / Aggregators
  - Road Network — ESRI / Generate on your own
- **Processing Steps:**
  - Combine Data — 2-4 wks
  - Standardize / Spatial Joins — 2-3 wks
  - Feature Engineering — 2-4 wks
  - ML Modeling — 3-6 wks
  - Score Candidates — 1-2 wks
  - Optimize Network — 2-4 wks
- **Output (red/warning boxes):**
  - Spreadsheet — Static, no interactivity, no map
  - or
  - Dashboard without flexibility — Can't explore, can't ask "what if"
  - or
  - Custom App — +10 more weeks of engineering
- **Bottom annotation:** Total: 6-18 months end-to-end

## Slide 10: What If (B10-WhatIf)
- **Title:** What if it didn't have to look like this?

// I like this. 
## Slide 11: Databricks Architecture (B11-DatabricksArch)
- **Eyebrow label:** THE DATABRICKS APPROACH
- **Title:** On Databricks
- **Subtitle:** Every bottleneck maps to a native capability on one platform.
- **Column headers:** DATA SOURCES | DATABRICKS PLATFORM | OUTPUT
- **Data Sources:**
  - Demographics
  - Foot Traffic
  - Competition
  - Road Network
- **Connector label:** Marketplace + Connectors
- **Databricks Platform Steps:**
  - [H3] H3 Spatial Indexing — Open Spatial Indexing, Index once, query forever
  - [SQL] Spatial SQL — Spatial Joins Now 17x Faster
  - [FS] Feature Store — Reusable, versioned features
  - [ML] MLflow + UC Registry — Track, version, deploy models
  - [APP] Databricks Apps + Lakebase — Serve directly to business users
- **Output box — Interactive Decision Making Platform:**
  - Maps + scenario modeling
  - AI-powered insights
  - Self-service for business users
- **Bottom annotation:** months -> weeks
 
## Slide 12: Close (B12-Close)
- **Title:** Let me show you.
- **CTA button:** [play icon] See the app in action



# notes: 
mistake -> liability