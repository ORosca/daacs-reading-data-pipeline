# read_v2_ua23umgc-2022-23_summary_table_for_item_selection.R
# ============================================================
# Create six summary tables for item selection in the planned
# reading study by counting eligible items across reading domains
# under response-count thresholds.
#
# Purpose:
# 1. Read item-level response-count summaries from
#    item_sample_size_by_demo.csv.
# 2. Derive each item's reading domain from the lowercase domain
#    code embedded in QID:
#       id = identifying details
#       in = inference
#       l  = language / vocabulary
#       p  = purpose / point of view
#       s  = summary / synthesis
# 3. Build six domain tables for the Summary Table for Item
#    Selection:
#       - IRT 2PL Fit: 300 responses
#       - IRT 2PL Fit: 150 responses
#       - Multivariable DIF: 200 per group
#       - Multivariable DIF: 50 per group
#       - Factor Level: 100 per level
#       - Factor Level: 40 per level
# 4. Save the six tables to an Excel workbook and a CSV file.
# ============================================================

library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(openxlsx)

project_dir <- "C:/Users/orosc/OneDrive/Read"

output_dir <- file.path(project_dir, "read_v2_umgc_ua_22_23_combined_qa_outputs")

item_file <- file.path(
  project_dir,
  "read_v2_umgc_ua_22_23_combined_qa_outputs",
  "item_sample_size_by_demo.csv"
)
item_sample_size_by_demo <- read_csv(item_file, show_col_types = FALSE)

# ----------------------------
# helpers
# ----------------------------

domain_from_qid <- function(x) {
  code <- str_match(x, "^Q\\d{3}([A-Za-z]+)\\d+$")[, 2]

  case_when(
    code == "id" ~ "identifying_details",
    code == "in" ~ "inference",
    code == "l"  ~ "language_vocabulary",
    code == "p"  ~ "purpose_point_of_view",
    code == "s"  ~ "summary_synthesis",
    TRUE ~ NA_character_
  )
}

domain_order <- c(
  "identifying_details",
  "inference",
  "language_vocabulary",
  "purpose_point_of_view",
  "summary_synthesis"
)

# ----------------------------
# build item lookup from QID
# ----------------------------

qid_lookup <- item_sample_size_by_demo %>%
  distinct(QID) %>%
  mutate(domain = domain_from_qid(QID))

# ----------------------------
# threshold metrics
# ----------------------------

overall_n <- item_sample_size_by_demo %>%
  group_by(QID, demographic) %>%
  summarise(n_responded = sum(n_responded), .groups = "drop") %>%
  group_by(QID) %>%
  summarise(overall_n = max(n_responded), .groups = "drop")

min_gender_military_n <- item_sample_size_by_demo %>%
  filter(
    demographic %in% c("gender", "military"),
    !is.na(group_value),
    group_value != "NA",
    group_value != ""
  ) %>%
  group_by(QID) %>%
  summarise(min_gender_military_n = min(n_responded), .groups = "drop")

min_any_factor_level_n <- item_sample_size_by_demo %>%
  filter(
    !is.na(group_value),
    group_value != "NA",
    group_value != ""
  ) %>%
  group_by(QID) %>%
  summarise(min_any_factor_level_n = min(n_responded), .groups = "drop")

summary_df <- qid_lookup %>%
  left_join(overall_n, by = "QID") %>%
  left_join(min_gender_military_n, by = "QID") %>%
  left_join(min_any_factor_level_n, by = "QID")

# ----------------------------
# function to make one table
# ----------------------------

make_domain_table <- function(df, metric_col, threshold) {
  out <- df %>%
    filter(.data[[metric_col]] >= threshold) %>%
    count(domain, name = "n_items")

  out <- tibble(domain = domain_order) %>%
    left_join(out, by = "domain") %>%
    mutate(
      n_items = if_else(is.na(n_items), 0L, as.integer(n_items))
    )

  bind_rows(
    out,
    tibble(domain = "Total", n_items = sum(out$n_items, na.rm = TRUE))
  )
}

# ----------------------------
# six tables
# ----------------------------

tbl_irt_300 <- make_domain_table(summary_df, "overall_n", 300)
tbl_irt_150 <- make_domain_table(summary_df, "overall_n", 150)

tbl_dif_200 <- make_domain_table(summary_df, "min_gender_military_n", 200)
tbl_dif_50  <- make_domain_table(summary_df, "min_gender_military_n", 50)

tbl_factor_100 <- make_domain_table(summary_df, "min_any_factor_level_n", 100)
tbl_factor_40  <- make_domain_table(summary_df, "min_any_factor_level_n", 40)

# inspect in console
tbl_irt_300
tbl_irt_150
tbl_dif_200
tbl_dif_50
tbl_factor_100
tbl_factor_40

# ----------------------------
# save to Excel
# ----------------------------

out_xlsx <- file.path(output_dir, "Summary_Tables_for_Item_Selection_read.xlsx")

wb <- createWorkbook()

addWorksheet(wb, "README")
writeData(
  wb, "README",
  data.frame(
    Note = c(
      "File used: item_sample_size_by_demo.csv",
      "Domain was derived from the lowercase domain code embedded in QID: id, in, l, p, s.",
      "Mapping used: id=identifying_details; in=inference; l=language_vocabulary; p=purpose_point_of_view; s=summary_synthesis.",
      "Reading items were not split by difficulty because the reading MST does not use item difficulty levels in the same way as math.",
      "IRT tables: overall item responses threshold.",
      "DIF tables: minimum item responses across gender and military groups threshold.",
      "Factor-level tables: minimum item responses across all non-missing factor levels threshold."
    )
  )
)

sheet_write <- function(wb, sheet, df, subtitle) {
  addWorksheet(wb, sheet)
  writeData(wb, sheet, subtitle, startRow = 1, startCol = 1)
  writeData(wb, sheet, df, startRow = 3, startCol = 1, withFilter = FALSE)

  hs <- createStyle(
    fontColour = "#FFFFFF",
    fgFill = "#1F4E78",
    halign = "center",
    textDecoration = "bold",
    border = "Bottom"
  )
  body <- createStyle(border = "Bottom")
  note <- createStyle(textDecoration = "italic")

  addStyle(wb, sheet, note, rows = 1, cols = 1, gridExpand = TRUE)
  addStyle(wb, sheet, hs, rows = 3, cols = 1:ncol(df), gridExpand = TRUE)
  addStyle(wb, sheet, body, rows = 4:(nrow(df) + 3), cols = 1:ncol(df), gridExpand = TRUE)

  setColWidths(wb, sheet, cols = 1:ncol(df), widths = c(28, 12))
  freezePane(wb, sheet, firstActiveRow = 4)
}

sheet_write(
  wb, "IRT_Frequentist_300", tbl_irt_300,
  "IRT 2PL Fit: minimum recommended frequentist threshold = 300 responses"
)
sheet_write(
  wb, "IRT_Bayesian_150", tbl_irt_150,
  "IRT 2PL Fit: minimum with Bayesian stabilization = 150 responses"
)

sheet_write(
  wb, "DIF_Frequentist_200", tbl_dif_200,
  "Multivariable DIF: minimum recommended frequentist threshold = 200 per gender/military group"
)
sheet_write(
  wb, "DIF_Bayesian_50", tbl_dif_50,
  "Multivariable DIF: minimum with Bayesian stabilization = 50 per gender/military group"
)

sheet_write(
  wb, "FactorLevel_Freq_100", tbl_factor_100,
  "Factor level: minimum recommended frequentist threshold = 100 per factor level"
)
sheet_write(
  wb, "FactorLevel_Bayes_40", tbl_factor_40,
  "Factor level: minimum with Bayesian stabilization = 40 per factor level"
)

saveWorkbook(wb, out_xlsx, overwrite = TRUE)
