# read_v2_ua_22_23_17-18_describe.R
# ============================================================
# Subset YoungerS UAlbany (traditional college age students, 
# TCAS, age = 17 or 18. Run final missingness diagnostics, 
# create descriptive summaries, item-level
# sample-size tables, and save the final cleaned dataset.
# ============================================================

suppressPackageStartupMessages({
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(tibble)
library(purrr)
library(ggplot2)
library(openxlsx)
})

project_dir <- "C:/Users/orosc/OneDrive/Read"
source(file.path(project_dir, "utils_read_pipeline.R"))

output_dir <- file.path(project_dir, 
                        "read_v2_ua_22_23_17-18_describe")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

input_file <- file.path(project_dir, 
        "read_v2_umgc_ua_22_23_combined_qa_outputs","read_v2_umgc_ua_22_23.rds")

stopifnot(file.exists(input_file))

# ------------------------------------------------------------
# Section 1. Load files and finalize cleaned dataset
# ------------------------------------------------------------

read_v2_umgc_ua_22_23 <- readRDS(input_file)

# Final cleaned dataset:
# remove the rows of students older than 18
read_v2_umgc_ua_22_23_17_18 <- 
  read_v2_umgc_ua_22_23[read_v2_umgc_ua_22_23$age == 17 |  
                          read_v2_umgc_ua_22_23$age == 18,]

# remove the rows of 25 umgc students
read_v2_ua_22_23_17_18 <- 
  read_v2_umgc_ua_22_23_17_18[read_v2_umgc_ua_22_23_17_18$college == "ua2" |  
                            read_v2_umgc_ua_22_23_17_18$college == "ua23" ,]

# Quick QA checks
stopifnot(sum(duplicated(read_v2_ua_22_23_17_18$global_id)) == 0)

n_input <- nrow(read_v2_ua_22_23_17_18)

cat("Input raw rows:   ", n_input, "\n")

read_v2_ua_22_23_17_18 <- read_v2_ua_22_23_17_18 %>%
  mutate(
    college   = as.factor(college),
    wave      = as.factor(wave),
    age       = as.factor(age),
    gender    = as.factor(gender),
    ethnicity = as.factor(ethnicity),
    military  = as.factor(military),
    pell      = as.factor(pell),
    transfer  = suppressWarnings(as.numeric(transfer)),
    readTime  = suppressWarnings(as.numeric(readTime))
  )

miss_nodup <- run_missingness_diagnostics(
  df = read_v2_ua_22_23_17_18,
  save_plots = TRUE,
  output_dir = output_dir
)

# ------------------------------------------------------------
# Section 2. Descriptive statistics
# ------------------------------------------------------------

cat_vars <- c("college", "wave", "age", "gender", "ethnicity", "military", "pell")

cat_descriptives <- lapply(cat_vars, function(v) {
  read_v2_ua_22_23_17_18 %>%
    count(.data[[v]], .drop = FALSE) %>%
    mutate(
      variable = v,
      percent = 100 * n / sum(n)
    ) %>%
    rename(category = 1) %>%
    select(variable, category, n, percent)
})

cat_descriptives_df <- bind_rows(cat_descriptives)

num_descriptives <- read_v2_ua_22_23_17_18 %>%
  summarise(
    transfer_n = sum(!is.na(transfer)),
    transfer_mean = mean(transfer, na.rm = TRUE),
    transfer_sd = sd(transfer, na.rm = TRUE),
    transfer_min = min(transfer, na.rm = TRUE),
    transfer_q1 = as.numeric(quantile(transfer, 0.25, na.rm = TRUE)),
    transfer_median = median(transfer, na.rm = TRUE),
    transfer_q3 = as.numeric(quantile(transfer, 0.75, na.rm = TRUE)),
    transfer_max = max(transfer, na.rm = TRUE),
    
    readTime_n = sum(!is.na(readTime)),
    readTime_mean = mean(readTime, na.rm = TRUE),
    readTime_sd = sd(readTime, na.rm = TRUE),
    readTime_min = min(readTime, na.rm = TRUE),
    readTime_q1 = as.numeric(quantile(readTime, 0.25, na.rm = TRUE)),
    readTime_median = median(readTime, na.rm = TRUE),
    readTime_q3 = as.numeric(quantile(readTime, 0.75, na.rm = TRUE)),
    readTime_max = max(readTime, na.rm = TRUE)
  )

p_transfer_hist <- ggplot(read_v2_ua_22_23_17_18, aes(x = transfer)) +
  geom_histogram(bins = 30) +
  labs(title = "Transferred Credits Distribution", x = "Transferred Credits", y = "Count") +
  theme_minimal()

p_readTime_hist <- ggplot(read_v2_ua_22_23_17_18, aes(x = readTime)) +
  geom_histogram(bins = 30) +
  labs(title = "Read Time Distribution", x = "Read Time", y = "Count") +
  theme_minimal()

p_transfer_box <- ggplot(read_v2_ua_22_23_17_18, aes(y = transfer)) +
  geom_boxplot() +
  labs(title = "Boxplot of Transferred Credits", y = "Transferred Credits") +
  theme_minimal()

p_readTime_box <- ggplot(read_v2_ua_22_23_17_18, aes(y = readTime)) +
  geom_boxplot() +
  labs(title = "Boxplot of Read Time", y = "Read Time") +
  theme_minimal()

# ------------------------------------------------------------
# Section 3. Save outputs
# ------------------------------------------------------------

save_both(read_v2_ua_22_23_17_18, output_dir, "read_v2_ua_22_23_17_18")

write.csv(
  cat_descriptives_df,
  file.path(output_dir, "categorical_descriptives_read_v2_ua_22_23_17_18.csv"),
  row.names = FALSE
)

write.csv(
  num_descriptives,
  file.path(output_dir, "numeric_descriptives_read_v2_ua_22_23_17_18.csv"),
  row.names = FALSE
)

ggsave(file.path(output_dir, "transfer_histogram.png"), p_transfer_hist, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "readTime_histogram.png"), p_readTime_hist, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "transfer_boxplot.png"), p_transfer_box, width = 5, height = 5, dpi = 300)
ggsave(file.path(output_dir, "readTime_boxplot.png"), p_readTime_box, width = 5, height = 5, dpi = 300)

# ============================================================
# Create six summary tables for item selection in the planned
# reading study by counting eligible items across reading domains
# under response-count thresholds.
# ============================================================

item_file <- file.path(output_dir, "item_sample_size_by_demo.csv")
item_sample_size_by_demo <- read_csv(item_file, show_col_types = FALSE)

# ----------------------------
# helpers
# ----------------------------

# Fixed: Now correctly extracts domain codes (id, in, l, p, s) even without trailing digits
domain_from_qid <- function(x) {
  # Strip prefix 'Q###' and any trailing digits
  code <- gsub("^Q\\d{3}", "", x)
  code <- gsub("\\d+$", "", code)
  code <- tolower(code)
  
  case_when(
    code == "id" ~ "ideas",
    code == "in" ~ "inference",
    code == "l"  ~ "language",
    code == "p"  ~ "purpose",
    code == "s"  ~ "structure", # Fixed spelling
    TRUE ~ NA_character_
  )
}

domain_order <- c(
  "ideas",
  "inference",
  "language",
  "purpose",
  "structure"
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

# Updated to specifically track gender and pell as requested
min_gender_pell_n <- item_sample_size_by_demo %>%
  filter(
    demographic %in% c("gender", "pell"),
    !is.na(group_value),
    group_value != "NA",
    group_value != ""
  ) %>%
  group_by(QID) %>%
  summarise(min_gender_pell_n = min(n_responded), .groups = "drop")

summary_df <- qid_lookup %>%
  left_join(overall_n, by = "QID") %>%
  left_join(min_gender_pell_n, by = "QID")

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

tbl_dif_200 <- make_domain_table(summary_df, "min_gender_pell_n", 200)
tbl_dif_50  <- make_domain_table(summary_df, "min_gender_pell_n", 50)

tbl_factor_100 <- make_domain_table(summary_df, "min_gender_pell_n", 100)
tbl_factor_40  <- make_domain_table(summary_df, "min_gender_pell_n", 40)

# ----------------------------
# save to Excel
# ----------------------------

out_xlsx <- file.path(output_dir, "Summary_Tables_for_Item_Selection_read.xlsx")
wb <- createWorkbook()

addWorksheet(wb, "README")
writeData(wb, "README", data.frame(
  Note = c(
    "File used: item_sample_size_by_demo.csv",
    "Domain was derived from the domain code embedded in QID: id, in, l, p, s.",
    "Mapping: id=ideas; in=inference; l=language; p=purpose; s=structure.",
    "IRT tables: overall item responses threshold.",
    "DIF and Factor Level tables: minimum item responses across gender and pell groups."
  )
))

sheet_write <- function(wb, sheet, df, subtitle) {
  addWorksheet(wb, sheet)
  writeData(wb, sheet, subtitle, startRow = 1, startCol = 1)
  writeData(wb, sheet, df, startRow = 3, startCol = 1)
  
  hs <- createStyle(fontColour = "#FFFFFF", fgFill = "#1F4E78", halign = "center", textDecoration = "bold", border = "Bottom")
  addStyle(wb, sheet, hs, rows = 3, cols = 1:ncol(df), gridExpand = TRUE)
  setColWidths(wb, sheet, cols = 1:ncol(df), widths = c(28, 12))
}

# Applied requested labels for Reading project
sheet_write(wb, "IRT_Frequentist_300", tbl_irt_300, 
            "IRT 2PL Fit: minimum frequentist threshold = 300 responses")

sheet_write(wb, "IRT_Bayesian_150", tbl_irt_150, 
            "IRT 2PL Fit: Bayesian stabilization = 150 responses")

sheet_write(wb, "DIF_Frequentist_200", tbl_dif_200, 
            "DIF: 200 per gender/pell group")

sheet_write(wb, "DIF_Bayesian_50", tbl_dif_50, 
            "DIF: Bayesian stabilization = 50 per gender/pell group")

sheet_write(wb, "FactorLevel_Freq_100", tbl_factor_100, 
            "Factor level: 100 per factor level on gender/pell")

sheet_write(wb, "FactorLevel_Bayes_40", tbl_factor_40, 
            "Factor level: Bayesian stabilization = 40 per factor level on gender/pell")

saveWorkbook(wb, out_xlsx, overwrite = TRUE)