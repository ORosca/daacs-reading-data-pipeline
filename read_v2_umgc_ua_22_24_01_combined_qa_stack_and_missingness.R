# read_v2_umgc_ua_22_24_01_combined_qa_stack_and_missingness.R
# ============================================================
# Combine 2022 and 2022-24 reading wide files, harmonize columns,
# and run missingness diagnostics on the stacked data.
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(ggplot2)
})

# ------------------------------------------------------------
# Section 1. Paths
# ------------------------------------------------------------

project_dir <- "C:/Users/orosc/OneDrive/Read"
source(file.path(project_dir, "utils_read_pipeline.R"))

file_2022 <- file.path(
  project_dir,
  "read_v2_umgc1ua2-anSamp2-2022_qa_outputs",
  "final_clean_datasets",
  "read_umgc1ua2_anSamp2_wide.rds"
)

file_2022_24 <- file.path(
  project_dir,
  "read_v2_ua23umgc-2022-24_qa_outputs",
  "read_ua23umgc_wide.rds"
)

output_dir <- file.path(project_dir, "read_v2_umgc_ua_22_24_combined_qa_outputs")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

stopifnot(file.exists(file_2022))
stopifnot(file.exists(file_2022_24))

# ------------------------------------------------------------
# Section 2. Load and harmonize
# ------------------------------------------------------------

wide_2022 <- readRDS(file_2022)
wide_2022_24 <- readRDS(file_2022_24)

if (!"wave" %in% names(wide_2022)) {
  wide_2022 <- wide_2022 %>% mutate(wave = 2022L)
}

wide_2022 <- wide_2022 %>%
  mutate(
    DAACS_ID = as.character(DAACS_ID),
    global_id = as.character(global_id),
    college = as.character(college),
    wave = as.integer(wave),
    source_file = "umgc1ua2_anSamp2_2022"
  )

wide_2022_24 <- wide_2022_24 %>%
  mutate(
    DAACS_ID = as.character(DAACS_ID),
    global_id = as.character(global_id),
    college = as.character(college),
    wave = as.integer(wave),
    source_file = "ua23umgc_2022_24"
  )

assert_has_cols(wide_2022, c("DAACS_ID", "global_id", "college", "wave"), "wide_2022")
assert_has_cols(wide_2022_24, c("DAACS_ID", "global_id", "college", "wave"), "wide_2022_24")

wide_2022 <- wide_2022 %>% coerce_factors_to_char() %>% coerce_item_cols()
wide_2022_24 <- wide_2022_24 %>% coerce_factors_to_char() %>% coerce_item_cols()

item_cols_all <- sort(unique(c(get_item_cols(wide_2022), get_item_cols(wide_2022_24))))
item_cols_all <- item_cols_all[order(substr(item_cols_all, 1, 4), item_cols_all)]

non_item_cols_all <- union(
  names(wide_2022)[!names(wide_2022) %in% get_item_cols(wide_2022)],
  names(wide_2022_24)[!names(wide_2022_24) %in% get_item_cols(wide_2022_24)]
)

master_cols <- c(non_item_cols_all, item_cols_all)

wide_2022 <- align_to_master_cols(wide_2022, master_cols)
wide_2022_24 <- align_to_master_cols(wide_2022_24, master_cols)

read_v2_umgc_ua_2waves4sets_raw <- bind_rows(wide_2022, wide_2022_24)

# ------------------------------------------------------------
# Section 3. Missingness diagnostics
# ------------------------------------------------------------

miss_raw <- run_missingness_diagnostics(
  df = read_v2_umgc_ua_2waves4sets_raw,
  stage_name = "raw",
  save_plots = TRUE,
  output_dir = output_dir
)

# ------------------------------------------------------------
# Section 4. Save outputs
# ------------------------------------------------------------

save_both(read_v2_umgc_ua_2waves4sets_raw, output_dir, "read_v2_umgc_ua_2waves4sets_raw")
