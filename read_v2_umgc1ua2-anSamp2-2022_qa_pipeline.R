# read_v2_umgc1ua2-anSamp2-2022_qa_pipeline.R
# ============================================================
# QA pipeline for the already-wide UMGC1 + UA2 AnSamp2 read file
#
# Purpose:
# 1. Load the already-wide 2022 UMGC1 + UA2 read dataset
# 2. Rename item columns from legacy qid_ua2 names to final QIDs
# 3. Standardize key demographic and metadata variables
# 4. Create clean UMGC1, UA2, and combined wide datasets
# 5. Run item- and dataset-level QA summaries
#
# Key conventions:
# - age_d24: TCAUS if age < 24, AUS if age >= 24
# - ethnicity: White / Asian / Black / Hispanic / Other
# - pell: No / Yes
# - military: No / Yes
# - transfer: continuous transferred credits
# - readTime: seconds
#
# Unit of analysis in final outputs:
# one row per student (global_id)
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(tidyr)
  library(purrr)
  library(tibble)
  library(ggplot2)
})

# ============================================================
# Section 1. Configuration
# ============================================================

project_dir <- "C:/Users/orosc/OneDrive/Read"
source(file.path(project_dir, "utils_read_pipeline.R"))

input_rds <- file.path(project_dir, "read.items_AnSamp2_umgc1ua2.rds")
input_csv <- NULL

mapping_xlsx <- file.path(project_dir, "MappingQID_read.xlsx")

output_dir <- file.path(
  project_dir,
  "read_v2_umgc1ua2-anSamp2-2022_qa_outputs"
)
final_output_dir <- file.path(output_dir, "final_clean_datasets")

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(final_output_dir, showWarnings = FALSE, recursive = TRUE)

WAVE_YEAR <- 2022L
ADULT_AGE_CUTOFF <- 24L

stopifnot(file.exists(input_rds) || (!is.null(input_csv) && file.exists(input_csv)))
stopifnot(file.exists(mapping_xlsx))

# ============================================================
# Section 2. Standardize the wide dataset
# ============================================================

standardize_wide_read <- 
  function(df, wave = WAVE_YEAR, adult_age_cutoff = ADULT_AGE_CUTOFF, deduplicate = TRUE) {
  assert_has_cols(df, c("DAACS_ID", "college"), "wide read data")

  df %>%
    mutate(
      college = tolower(as.character(college)),
      college = case_when(
        college %in% c("umgc1", "umgc") ~ "umgc1",
        college %in% c("ua2", "ua") ~ "ua2",
        TRUE ~ college
      ),

      DAACS_ID = normalize_daacs_id(DAACS_ID),

      # restore original UA2 DAACS_ID by subtracting 10000
      DAACS_ID_num = suppressWarnings(as.numeric(DAACS_ID)),
      DAACS_ID_num = case_when(
        college == "ua2" & !is.na(DAACS_ID_num) & DAACS_ID_num >= 10000 ~ DAACS_ID_num - 10000,
        TRUE ~ DAACS_ID_num
      ),
      DAACS_ID = if_else(
        !is.na(DAACS_ID_num),
        sprintf("%08d", as.integer(DAACS_ID_num)),
        DAACS_ID
      ),

      wave = as.integer(wave),
      global_id = make_global_id(college, wave, DAACS_ID),

      age = if ("Age" %in% names(.)) suppressWarnings(as.numeric(Age)) else NA_real_,
      age_d24 = case_when(
        is.na(age) ~ NA_character_,
        age < adult_age_cutoff ~ "TCAUS",
        age >= adult_age_cutoff ~ "AUS",
        TRUE ~ NA_character_
      ),
      age_d24 = factor(age_d24, levels = c("TCAUS", "AUS")),

      gender = if ("gender" %in% names(.)) as.character(gender) else NA_character_,
      gender = case_when(
        gender %in% c("Male", "Female") ~ gender,
        TRUE ~ NA_character_
      ),

      ethnicity = if ("ethnicity" %in% names(.)) recode_ethnicity_common(ethnicity) else NA_character_,
      ethnicity = factor(
        ethnicity,
        levels = c("White", "Asian", "Black", "Hispanic", "Other")
      ),

      military = if ("Military" %in% names(.)) recode_yes_no(Military) else NA_character_,
      military = factor(military, levels = c("No", "Yes")),

      pell = if ("Pell" %in% names(.)) recode_yes_no(Pell) else NA_character_,
      pell = factor(pell, levels = c("No", "Yes")),

      # continuous transferred credits for consistency with later waves
      transfer = if ("credits_transferred" %in% names(.)) {
        suppressWarnings(as.numeric(credits_transferred))
      } else {
        NA_real_
      },

      credits_transferred = if ("credits_transferred" %in% names(.)) {
        suppressWarnings(as.numeric(credits_transferred))
      } else {
        NA_real_
      },

      readCompletionDate = if ("readCompletionDate" %in% names(.)) {
        as.POSIXct(readCompletionDate)
      } else {
        as.POSIXct(NA)
      },

      # keep readTime explicitly in seconds
      readTime = if ("readTime" %in% names(.)) {
        suppressWarnings(as.integer(readTime))
      } else {
        NA_integer_
      }
    ) %>%
    select(
      DAACS_ID, global_id, college, wave,
      age, age_d24, gender, ethnicity, military, pell, transfer,
      everything(),
      -DAACS_ID_num,
      -any_of(c("Age", "Age_d24", "Military", "Pell"))
    ) %>%
    reorder_item_columns() %>%
    { if (deduplicate) distinct(., global_id, .keep_all = TRUE) else . }
  }

# ============================================================
# Section 3. Load, clean, split, and save
# ============================================================

wide_raw <- load_wide_input(input_rds, input_csv)
qid_map <- load_qid_mapping(mapping_xlsx)

wide_renamed <- rename_item_columns_to_final_qid(wide_raw, qid_map)
wide_std <- standardize_wide_read(
  wide_renamed,
  wave = WAVE_YEAR,
  deduplicate = FALSE
)

pre_dedup_duplicates <- qa_duplicates(wide_std)

wide_dedup <- wide_std %>%
  distinct(global_id, .keep_all = TRUE)

speed_filter <- filter_speedy_read_students(
  wide_dedup,
  min_items = 18L,
  min_seconds = 210L
)

wide_clean <- speed_filter$data

saveRDS(
  speed_filter,
  file.path(output_dir, "speedy_read_filter_results.rds")
)

write.csv(
  speed_filter$summary,
  file.path(output_dir, "speedy_read_filter_summary.csv"),
  row.names = FALSE
)

write.csv(
  speed_filter$flagged_cases,
  file.path(output_dir, "speedy_read_flagged_cases.csv"),
  row.names = FALSE
)

saveRDS(
  pre_dedup_duplicates,
  file.path(output_dir, "pre_dedup_duplicate_summary.rds")
)

write.csv(
  pre_dedup_duplicates$summary,
  file.path(output_dir, "pre_dedup_duplicate_summary.csv"),
  row.names = FALSE
)

write.csv(
  pre_dedup_duplicates$duplicate_values,
  file.path(output_dir, "pre_dedup_duplicate_values.csv"),
  row.names = FALSE
)

read_umgc1_anSamp2_wide <- wide_clean %>%
  filter(college == "umgc1") %>%
  distinct(global_id, .keep_all = TRUE)

read_ua2_anSamp2_wide <- wide_clean %>%
  filter(college == "ua2") %>%
  distinct(global_id, .keep_all = TRUE)

read_umgc1ua2_anSamp2_wide <- wide_clean %>%
  distinct(global_id, .keep_all = TRUE)

save_both(read_umgc1_anSamp2_wide, final_output_dir, "read_umgc1_anSamp2_wide")
save_both(read_ua2_anSamp2_wide, final_output_dir, "read_ua2_anSamp2_wide")
save_both(read_umgc1ua2_anSamp2_wide, final_output_dir, "read_umgc1ua2_anSamp2_wide")

# ============================================================
# Section 4. Run QA on each clean dataset
# ============================================================

qa_umgc1_anSamp2 <- run_qa_wide_read(
  df = read_umgc1_anSamp2_wide,
  dataset_name = "read_umgc1_anSamp2_wide",
  output_dir = file.path(output_dir, "qa_umgc1_anSamp2")
)

qa_ua2_anSamp2 <- run_qa_wide_read(
  df = read_ua2_anSamp2_wide,
  dataset_name = "read_ua2_anSamp2_wide",
  output_dir = file.path(output_dir, "qa_ua2_anSamp2")
)

qa_umgc1ua2_combined <- run_qa_wide_read(
  df = read_umgc1ua2_anSamp2_wide,
  dataset_name = "read_umgc1ua2_anSamp2_wide",
  output_dir = file.path(output_dir, "qa_umgc1ua2_combined")
)

# ============================================================
# Section 5. Final checks and console summary
# ============================================================

stopifnot(sum(duplicated(read_umgc1_anSamp2_wide$global_id)) == 0)
stopifnot(sum(duplicated(read_ua2_anSamp2_wide$global_id)) == 0)
stopifnot(sum(duplicated(read_umgc1ua2_anSamp2_wide$global_id)) == 0)

stopifnot(length(get_item_cols(read_umgc1_anSamp2_wide)) > 0)
stopifnot(length(get_item_cols(read_ua2_anSamp2_wide)) > 0)
stopifnot(length(get_item_cols(read_umgc1ua2_anSamp2_wide)) > 0)

cat("\nSaved clean datasets to:\n", final_output_dir, "\n")

cat("\nRows:\n")
cat("UMGC1: ", nrow(read_umgc1_anSamp2_wide), "\n")
cat("UA2:   ", nrow(read_ua2_anSamp2_wide), "\n")
cat("Combined: ", nrow(read_umgc1ua2_anSamp2_wide), "\n")

cat("\nDuplicate global_id counts:\n")
cat(
  "UMGC1: ",
  qa_umgc1_anSamp2$duplicates$summary$value[
    qa_umgc1_anSamp2$duplicates$summary$metric == "duplicate_global_id_n"
  ],
  "\n"
)
cat(
  "UA2:   ",
  qa_ua2_anSamp2$duplicates$summary$value[
    qa_ua2_anSamp2$duplicates$summary$metric == "duplicate_global_id_n"
  ],
  "\n"
)
cat(
  "Combined: ",
  qa_umgc1ua2_combined$duplicates$summary$value[
    qa_umgc1ua2_combined$duplicates$summary$metric == "duplicate_global_id_n"
  ],
  "\n"
)

