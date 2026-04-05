# read_v2_ua23umgc-2022-23_qa_pipeline.R
# ============================================================
# QA pipeline for UA23 + UMGC reading datasets (2022-2023)
#
# Purpose:
# 1. Load institution-level, item-level, and assessment-template files
# 2. Standardize institution-level demographics and identifiers
# 3. Map assessment-template items to global QIDs using exact and fuzzy domain-constrained stem matching
# 4. Attach template rows and QIDs to item-level responses using question_id
# 5. Remove same-completion duplicate item rows
# 6. Retain the first 18 cleaned responses per student
# 7. Derive student-level readCompletionDate and readTime
# 8. Exclude students with non-exactly-18 or duplicate-question retained responses
# 9. Exclude speedy students (< 3.5 minutes for 18 retained items)
# 10. Require all 18 retained responses to map to global QIDs
# 11. Build clean long and wide reading datasets for UA23, UMGC, and combined
# 12. Export QA summaries and cleaned outputs
#
# This pipeline prioritizes analysis-ready comparability across waves/institutions over preservation of all raw response history
# Item identity harmonization uses exact matching first, then constrained fuzzy matching, then manual review
# Exclusions are designed to remove records that would threaten defensible score interpretation, not to maximize sample size
#
# Key conventions:
# - age_d24: TCAUS if age < 24, AUS if age >= 24
# - ethnicity: White / Asian / Black / Hispanic / Other
# - pell: No / Yes
# - military: No / Yes
# - transfer: continuous transferred credits
# - readTime: seconds
#
# Unit of analysis:
# - long files: one row per student-item response
# - wide files: one row per student (global_id)
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(tibble)
  library(lubridate)
  library(ggplot2)
  library(purrr)
  library(stringdist)
})

# ============================================================
# Section 0. Configuration
# ============================================================

project_dir <- "C:/Users/orosc/OneDrive/Read"
source(file.path(project_dir, "utils_read_pipeline.R"))

map_file <- file.path(project_dir, "MappingQID_read.xlsx")

institution_ua23_file <- file.path("D:/DData23/institution.ua23.treat.rds")
institution_umgc_file <- file.path("D:/DData23/institution.umgc.treat.rds")

items_ua23_file <- file.path("D:/DData23/read.item.results.ua23.treat.rds")
items_umgc_file <- file.path("D:/DData23/read.item.results.umgc.treat.rds")

assess_ua23_file <- file.path("D:/DData23/read.assessments.ua23.rds")
assess_umgc_file <- file.path("D:/DData23/read.assessments.umgc.rds")

llm_output_dir <- file.path(project_dir, "read_llm_audit_outputs")
llm_patch_file <- file.path(llm_output_dir, "llm_patch_final_read.csv")

USE_LLM_REPAIR <- file.exists(llm_patch_file)

output_dir <- file.path(project_dir, "read_v2_ua23umgc-2022-23_qa_outputs")
qa_dir <- file.path(output_dir, "qa_wide")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(qa_dir, showWarnings = FALSE, recursive = TRUE)

MIN_ITEMS <- 18L
MIN_SECONDS_READ <- 210L   # 3.5 minutes
UA23_WAVE <- 2023L
ADULT_AGE_CUTOFF <- 24L

stopifnot(file.exists(map_file))
stopifnot(file.exists(institution_ua23_file))
stopifnot(file.exists(institution_umgc_file))
stopifnot(file.exists(items_ua23_file))
stopifnot(file.exists(items_umgc_file))
stopifnot(file.exists(assess_ua23_file))
stopifnot(file.exists(assess_umgc_file))

# ============================================================
# Section 1. Load files
# ============================================================

map_raw <- read_excel(map_file)

institution.ua23.treat <- readRDS(institution_ua23_file)
institution.umgc.treat <- readRDS(institution_umgc_file)

read.item.results.ua23.treat <- readRDS(items_ua23_file)
read.item.results.umgc.treat <- readRDS(items_umgc_file)

read.assessments.ua23 <- readRDS(assess_ua23_file)
read.assessments.umgc <- readRDS(assess_umgc_file)

# ============================================================
# Section 2. QID matching
# ============================================================

normalize_text_key <- function(x) {
  x <- as.character(x)
  x <- gsub("<[^>]+>", " ", x)              # remove HTML tags
  x <- stringr::str_replace_all(x, "&nbsp;|&#160;", " ")
  x <- stringr::str_replace_all(x, "&amp;", "&")
  x <- stringr::str_replace_all(x, "&quot;", "\"")
  x <- stringr::str_replace_all(x, "“|”", "\"")
  x <- stringr::str_replace_all(x, "‘|’", "'")
  x <- stringr::str_replace_all(x, "—|–", "-")
  x <- stringr::str_replace_all(x, "\\.{3,}", " ")
  x <- stringr::str_replace_all(x, "[[:space:]]+", " ")
  x <- stringr::str_trim(x)
  x <- tolower(x)
  x[x %in% c("", "na", "null", "<na>")] <- NA_character_
  x
}

normalize_domain_key <- function(x) {
  x <- as.character(x)
  x <- stringr::str_trim(x)
  x <- tolower(x)
  x[x %in% c("", "na", "null", "<na>")] <- NA_character_
  x
}

standardize_read_assessment_template <- function(df, source_name) {
  assert_has_cols(
    df,
    c("assessment_id", "block", "question_id", "domainId", "stem", "question",
      "a", "b", "c", "d", "answer"),
    source_name
  )
  
  df %>%
    mutate(
      source_name = source_name,
      assessment_id = as.character(assessment_id),
      question_id = as.character(question_id),
      block = as.integer(block),
      domainId = normalize_domain_key(domainId),
      stem = as.character(stem),
      question = as.character(question),
      a = as.character(a),
      b = as.character(b),
      c = as.character(c),
      d = as.character(d),
      answer = as.character(answer),
      difficulty = if ("difficulty" %in% names(.)) as.character(difficulty) else NA_character_,
      
      stem_key = normalize_text_key(stem),
      question_key = normalize_text_key(question)
    ) %>%
    distinct(question_id, .keep_all = TRUE)
}

# a 3-step rule:
# exact domain + stem match
# fuzzy stem match within domain only
# manual repair for remaining unmatched template rows

build_read_qid_lookup <- function(map_raw, assess_ua23, assess_umgc,
                                  max_distance = 0.12) {# this threshold is intentionally conservative 
  assess_bank <- bind_rows(
    standardize_read_assessment_template(assess_ua23, "ua23"),
    standardize_read_assessment_template(assess_umgc, "umgc")
  ) %>%
    distinct(source_name, question_id, .keep_all = TRUE) %>%
    mutate(
      domain_key = normalize_domain_key(domainId),
      stem_key = normalize_text_key(stem)
    )
  
  map_std <- map_raw %>%
    mutate(
      domain_key = normalize_domain_key(domain),
      stem_key = normalize_text_key(stem),
      QID = as.character(QID)
    ) %>%
    distinct(QID, .keep_all = TRUE)
  
# ============================================================
# Section 2A. Exact Match
# ============================================================
  
  exact_matched <- assess_bank %>%
    left_join(
      map_std %>%
        select(QID, domain_key, stem_key),
      by = c("domain_key", "stem_key")
    ) %>%
    mutate(
      match_method = if_else(!is.na(QID), "exact_domain_stem", NA_character_),
      string_distance = if_else(!is.na(QID), 0, NA_real_)
    )
  
  unmatched_exact <- exact_matched %>%
    filter(is.na(QID))

# ============================================================
# Section 2B. Fuzzy fallback within domain
# ============================================================

  fuzzy_candidates <- unmatched_exact %>%
    select(source_name, question_id, assessment_id, block, domainId, difficulty,
           stem, question, domain_key, stem_key) %>%
    inner_join(
      map_std %>%
        select(QID, domain_key, stem_key),
      by = "domain_key",
      relationship = "many-to-many"
    ) %>%
    mutate(
      string_distance = stringdist::stringdist(stem_key.x, stem_key.y, method = "jw")
    ) %>%
    group_by(source_name, question_id) %>%
    slice_min(order_by = string_distance, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    mutate(
      QID = if_else(string_distance <= max_distance, QID, NA_character_),
      match_method = if_else(string_distance <= max_distance,
                             "fuzzy_domain_stem", "unmatched")
    ) %>%
    transmute(
      source_name, question_id,
      QID_fuzzy = QID,
      fuzzy_distance = string_distance,
      fuzzy_method = match_method
    )
  
  template_qid <- exact_matched %>%
    left_join(
      fuzzy_candidates,
      by = c("source_name", "question_id")
    ) %>%
    mutate(
      QID = coalesce(QID, QID_fuzzy),
      match_method = case_when(
        !is.na(match_method) ~ match_method,
        !is.na(QID_fuzzy) ~ "fuzzy_domain_stem",
        TRUE ~ "unmatched"
      ),
      string_distance = case_when(
        match_method == "exact_domain_stem" ~ 0,
        !is.na(fuzzy_distance) ~ fuzzy_distance,
        TRUE ~ NA_real_
      )
    ) %>%
    select(
      source_name, assessment_id, block, question_id, domainId, difficulty,
      stem, question, stem_key, question_key, QID, match_method, string_distance
    )
  
  match_diag <- tibble::tibble(
    metric = c(
      "template_rows",
      "distinct_template_questions",
      "matched_qid_n",
      "matched_exact_n",
      "matched_fuzzy_n",
      "unmatched_qid_n"
    ),
    value = c(
      nrow(template_qid),
      dplyr::n_distinct(template_qid$question_id),
      sum(!is.na(template_qid$QID)),
      sum(template_qid$match_method == "exact_domain_stem", na.rm = TRUE),
      sum(template_qid$match_method == "fuzzy_domain_stem", na.rm = TRUE),
      sum(is.na(template_qid$QID))
    )
  )
  
  list(
    template_qid = template_qid,
    match_diag = match_diag,
    unmatched = template_qid %>% filter(is.na(QID)),
    fuzzy_review = template_qid %>%
      filter(match_method == "fuzzy_domain_stem") %>%
      arrange(desc(string_distance), source_name, domainId, question_id)
  )
}

qid_lookup_obj <- build_read_qid_lookup(
  map_raw = map_raw,
  assess_ua23 = read.assessments.ua23,
  assess_umgc = read.assessments.umgc,
  max_distance = 0.12
)

safe_write_csv(
  qid_lookup_obj$match_diag,
  file.path(output_dir, "qid_lookup_match_diag.csv"),
  row.names = FALSE
)

safe_write_csv(
  qid_lookup_obj$unmatched,
  file.path(output_dir, "template_qid_unmatched.csv"),
  row.names = FALSE
)

safe_write_csv(
  qid_lookup_obj$fuzzy_review,
  file.path(output_dir, "template_qid_fuzzy_review.csv"),
  row.names = FALSE
)

template_qid_audit <- qid_lookup_obj$template_qid %>%
  distinct(source_name, question_id, domainId, stem, QID, match_method) %>%
  arrange(is.na(QID), source_name, domainId, stem)

safe_write_csv(
  template_qid_audit,
  file.path(output_dir, "template_qid_audit.csv"),
  row.names = FALSE
)

template_unmatched <- template_qid_audit %>%
  filter(is.na(QID))

safe_write_csv(
  template_unmatched,
  file.path(output_dir, "template_qid_unmatched.csv"),
  row.names = FALSE
)

nrow(template_unmatched)

# ============================================================
# Section 2C. Manual repair 
# ============================================================

# manual repair is restricted to cases that fail automatic matching,
# it is auditable because repaired rows are written out,
# and it is preferable to silent auto-assignment for item identity

manual_repair_file <- file.path(
  project_dir,
  "template_qid_unmatched_manual_repair.csv"
)

stopifnot(file.exists(manual_repair_file))

names(readr::read_csv(manual_repair_file, show_col_types = FALSE))

manual_repair <- readr::read_csv(manual_repair_file, show_col_types = FALSE)

# Patch the automatically produced template_qid table
template_qid_repaired <- qid_lookup_obj$template_qid %>%
  left_join(
    manual_repair %>%
      rename(QID_manual = QID),
    by = c("source_name", "question_id")
  ) %>%
  mutate(
    QID_original = QID,
    match_method_original = if ("match_method" %in% names(.)) match_method else NA_character_,
    string_distance_original = if ("string_distance" %in% names(.)) string_distance else NA_real_,
    QID = dplyr::coalesce(QID_manual, QID),
    match_method = dplyr::case_when(
      !is.na(QID_manual) ~ "manual_repair",
      !is.na(match_method_original) ~ match_method_original,
      !is.na(QID) ~ "auto_match",
      TRUE ~ "unmatched"
    ),
    string_distance = dplyr::case_when(
      !is.na(QID_manual) ~ NA_real_,
      TRUE ~ string_distance_original
    )
  ) %>%
  select(-QID_manual)

# overwrite the object
qid_lookup_obj$template_qid <- template_qid_repaired

# check that repair actually worked:
table(qid_lookup_obj$template_qid$match_method, useNA = "ifany")
sum(is.na(qid_lookup_obj$template_qid$QID))

# Recompute diagnostics after manual repair
qid_lookup_obj$match_diag_after_manual <- tibble::tibble(
  metric = c(
    "template_rows",
    "distinct_template_questions",
    "matched_qid_n_after_manual",
    "manual_repair_n",
    "still_unmatched_qid_n"
  ),
  value = c(
    nrow(qid_lookup_obj$template_qid),
    dplyr::n_distinct(qid_lookup_obj$template_qid$question_id),
    sum(!is.na(qid_lookup_obj$template_qid$QID)),
    sum(qid_lookup_obj$template_qid$match_method == "manual_repair", na.rm = TRUE),
    sum(is.na(qid_lookup_obj$template_qid$QID))
  )
)

qid_lookup_obj$still_unmatched_after_manual <- qid_lookup_obj$template_qid %>%
  filter(is.na(QID))

# ============================================================
# Section 2D. LLM-assisted human-reviewed repair
# ============================================================

llm_repair <- load_llm_repair_table(llm_patch_file)

if (!is.null(llm_repair) && nrow(llm_repair) > 0) {
  
  template_qid_llm_repaired <- template_qid_repaired %>%
    left_join(
      llm_repair %>%
        rename(QID_llm = QID),
      by = c("source_name", "question_id")
    ) %>%
    mutate(
      QID_pre_llm = QID,
      match_method_pre_llm = match_method,
      string_distance_pre_llm = string_distance,
      
      QID = dplyr::coalesce(QID_llm, QID),
      
      match_method = dplyr::case_when(
        !is.na(QID_llm) ~ "llm_human_review",
        TRUE ~ match_method
      ),
      
      string_distance = dplyr::case_when(
        !is.na(QID_llm) ~ NA_real_,
        TRUE ~ string_distance
      )
    ) %>%
    select(-QID_llm)
  
  qid_lookup_obj$template_qid <- template_qid_llm_repaired
  
} else {
  qid_lookup_obj$template_qid <- template_qid_repaired
}

qid_lookup_obj$match_diag_final <- tibble::tibble(
  metric = c(
    "template_rows",
    "distinct_template_questions",
    "matched_qid_n_final",
    "matched_exact_n_final",
    "matched_fuzzy_n_final",
    "manual_repair_n_final",
    "llm_human_review_n_final",
    "still_unmatched_qid_n_final"
  ),
  value = c(
    nrow(qid_lookup_obj$template_qid),
    dplyr::n_distinct(qid_lookup_obj$template_qid$question_id),
    sum(!is.na(qid_lookup_obj$template_qid$QID)),
    sum(qid_lookup_obj$template_qid$match_method == "exact_domain_stem", na.rm = TRUE),
    sum(qid_lookup_obj$template_qid$match_method == "fuzzy_domain_stem", na.rm = TRUE),
    sum(qid_lookup_obj$template_qid$match_method == "manual_repair", na.rm = TRUE),
    sum(qid_lookup_obj$template_qid$match_method == "llm_human_review", na.rm = TRUE),
    sum(is.na(qid_lookup_obj$template_qid$QID))
  )
)

qid_lookup_obj$still_unmatched_final <- qid_lookup_obj$template_qid %>%
  filter(is.na(QID))

safe_write_csv(
  qid_lookup_obj$match_diag_final,
  file.path(output_dir, "qid_lookup_match_diag_final.csv"),
  row.names = FALSE
)

safe_write_csv(
  qid_lookup_obj$template_qid %>%
    distinct(
      source_name, question_id, assessment_id, domainId, difficulty,
      stem, question, QID, match_method, string_distance
    ),
  file.path(output_dir, "template_qid_final.csv"),
  row.names = FALSE
)

safe_write_csv(
  qid_lookup_obj$still_unmatched_final,
  file.path(output_dir, "template_qid_still_unmatched_final.csv"),
  row.names = FALSE
)

cat("\nFinal harmonization applied.\n")
cat("Manual repair rows: ",
    sum(qid_lookup_obj$template_qid$match_method == 'manual_repair', na.rm = TRUE), "\n")
cat("LLM + human review rows: ",
    sum(qid_lookup_obj$template_qid$match_method == 'llm_human_review', na.rm = TRUE), "\n")
cat("Still unmatched final: ",
    sum(is.na(qid_lookup_obj$template_qid$QID)), "\n")

# ============================================================
# Section 3. Standardize institution-level data
# ============================================================

standardize_institution_umgc_read <- function(df, adult_age_cutoff = ADULT_AGE_CUTOFF) {
  df %>%
    mutate(
      DAACS_ID = normalize_daacs_id(DAACS_ID),
      wave = as.integer(lubridate::year(coalesce(FYE_EndDate, FYE_StartDate))),
      global_id = make_global_id("umgc", wave, DAACS_ID),
      college = "umgc",
      
      age = suppressWarnings(as.numeric(Age)),
      age_d24 = case_when(
        is.na(age) ~ NA_character_,
        age < adult_age_cutoff ~ "TCAUS",
        age >= adult_age_cutoff ~ "AUS",
        TRUE ~ NA_character_
      ),
      age_d24 = factor(age_d24, levels = c("TCAUS", "AUS")),
      
      gender = case_when(
        as.character(Gender) %in% c("Male", "Female") ~ as.character(Gender),
        TRUE ~ NA_character_
      ),
      
      ethnicity = recode_ethnicity_common(Ethnicity),
      ethnicity = factor(
        ethnicity,
        levels = c("White", "Asian", "Black", "Hispanic", "Other")
      ),
      
      military = recode_yes_no(MilitaryStudent),
      military = factor(military, levels = c("No", "Yes")),
      
      pell = recode_yes_no(PELL),
      pell = factor(pell, levels = c("No", "Yes")),
      
      transfer = suppressWarnings(as.numeric(Transfer_Credits)),
      
      treat = as.character(Treat),
      fye_course = as.character(FYE_Course),
      fye_semester = as.character(FYE_Semester)
    ) %>%
    distinct(global_id, .keep_all = TRUE)
}

standardize_institution_ua23_read <- function(df, wave = UA23_WAVE, adult_age_cutoff = ADULT_AGE_CUTOFF) {
  df %>%
    mutate(
      DAACS_ID = normalize_daacs_id(DAACS_ID),
      wave = as.integer(wave),
      global_id = make_global_id("ua23", wave, DAACS_ID),
      college = "ua23",
      
      age = suppressWarnings(as.numeric(Age)),
      age_d24 = case_when(
        is.na(age) ~ NA_character_,
        age < adult_age_cutoff ~ "TCAUS",
        age >= adult_age_cutoff ~ "AUS",
        TRUE ~ NA_character_
      ),
      age_d24 = factor(age_d24, levels = c("TCAUS", "AUS")),
      
      gender = case_when(
        as.character(Gender) %in% c("Male", "Female") ~ as.character(Gender),
        TRUE ~ NA_character_
      ),
      
      ethnicity = recode_ethnicity_common(Ethnicity),
      ethnicity = factor(
        ethnicity,
        levels = c("White", "Asian", "Black", "Hispanic", "Other")
      ),
      
      military = case_when(
        is.na(Military) ~ NA_character_,
        Military ~ "Yes",
        !Military ~ "No"
      ),
      military = factor(military, levels = c("No", "Yes")),
      
      pell = case_when(
        is.na(PELL) ~ NA_character_,
        PELL ~ "Yes",
        !PELL ~ "No"
      ),
      pell = factor(pell, levels = c("No", "Yes")),
      
      transfer = suppressWarnings(as.numeric(Transfer_Credits)),
      
      treat = as.character(Treat),
      fye_course = as.character(FYE_Course),
      fye_semester = as.character(FYE_Semester)
    ) %>%
    distinct(global_id, .keep_all = TRUE)
}

institution_ua23_std <- standardize_institution_ua23_read(institution.ua23.treat, wave = UA23_WAVE)
institution_umgc_std <- standardize_institution_umgc_read(institution.umgc.treat)

# ============================================================
# Section 4. Standardize item rows and clean response history
# ============================================================

standardize_read_item_results_raw <- function(df, college_name, template_qid) {
  assert_has_cols(
    df,
    c("DAACS_ID", "assessment_id", "question_id", "block", "domainId",
      "score", "startDate"),
    college_name
  )
  
  completion_var <- if ("completeDate" %in% names(df)) "completeDate" else "completionDate"
  
  df %>%
    mutate(
      DAACS_ID = normalize_daacs_id(DAACS_ID),
      assessment_id = as.character(assessment_id),
      question_id = as.character(question_id),
      block = as.integer(block),
      domainId = normalize_domain_key(domainId),
      score = suppressWarnings(as.integer(score)),
      college = college_name,
      source_name = college_name,
      startDate = as.POSIXct(startDate),
      completion_dt = as.POSIXct(.data[[completion_var]]),
      wave = as.integer(lubridate::year(coalesce(completion_dt, startDate))),
      global_id = make_global_id(college_name, wave, DAACS_ID)
    ) %>%
    left_join(
      template_qid %>%
        select(
          source_name,
          question_id,
          template_stem = stem,
          template_domain = domainId,
          QID,
          difficulty,
          match_method,
          string_distance
        ),
      by = c("source_name", "question_id")
    )
}

audit_same_completion_duplicates <- function(items_std) {
  items_std %>%
    count(global_id, completion_dt, question_id, name = "n_rows") %>%
    filter(n_rows > 1) %>%
    arrange(desc(n_rows), global_id, completion_dt, question_id)
}

deduplicate_read_items_same_completion <- function(items_std) {
  items_std %>%
    arrange(global_id, completion_dt, startDate, block, question_id) %>%
    distinct(global_id, completion_dt, question_id, .keep_all = TRUE)
}

make_read_attempt_count_audit <- function(items_std) {
  items_std %>%
    count(global_id, name = "n_item_rows") %>%
    mutate(
      row_pattern = case_when(
        n_item_rows == 18 ~ "single_18",
        n_item_rows == 36 ~ "double_36",
        n_item_rows %% 18 == 0 ~ "multiple_of_18_other",
        TRUE ~ "other"
      )
    ) %>%
    arrange(global_id)
}

# the target retained form is a single 18-response reading administration,
# duplicate same-completion rows are removed first,
# then the earliest complete attempt is retained for comparability across administrations,
# later responses are not mixed into the retained attempt.

retain_first_read_attempt <- function(items_std, n_items_per_attempt = 18L) {
  items_std %>%
    arrange(global_id, startDate, completion_dt, block, question_id) %>%
    group_by(global_id) %>%
    mutate(response_order = row_number()) %>%
    filter(response_order <= n_items_per_attempt) %>%
    ungroup()
}

diag_questionid_join <- function(items_std0, label) {
  items_std0 %>%
    summarise(
      dataset = label,
      n_rows = n(),
      prop_template_found = mean(!is.na(template_stem)),
      prop_qid_found = mean(!is.na(QID)),
      prop_qid_missing = mean(is.na(QID))
    ) %>%
    print()
}

items_ua23_std0 <- standardize_read_item_results_raw(
  df = read.item.results.ua23.treat,
  college_name = "ua23",
  template_qid = qid_lookup_obj$template_qid
)

items_umgc_std0 <- standardize_read_item_results_raw(
  df = read.item.results.umgc.treat,
  college_name = "umgc",
  template_qid = qid_lookup_obj$template_qid
)

diag_questionid_join(items_ua23_std0, "ua23")
diag_questionid_join(items_umgc_std0, "umgc")

ua23_same_completion_dups <- audit_same_completion_duplicates(items_ua23_std0)
umgc_same_completion_dups <- audit_same_completion_duplicates(items_umgc_std0)

write.csv(
  ua23_same_completion_dups,
  file.path(output_dir, "ua23_same_completion_duplicate_items.csv"),
  row.names = FALSE
)

write.csv(
  umgc_same_completion_dups,
  file.path(output_dir, "umgc_same_completion_duplicate_items.csv"),
  row.names = FALSE
)

# clean duplicate rows FIRST
items_ua23_clean <- deduplicate_read_items_same_completion(items_ua23_std0)
items_umgc_clean <- deduplicate_read_items_same_completion(items_umgc_std0)

ua23_attempt_audit_after_dedup <- make_read_attempt_count_audit(items_ua23_clean)
umgc_attempt_audit_after_dedup <- make_read_attempt_count_audit(items_umgc_clean)

write.csv(
  ua23_attempt_audit_after_dedup,
  file.path(output_dir, "ua23_attempt_count_audit_after_dedup.csv"),
  row.names = FALSE
)

write.csv(
  umgc_attempt_audit_after_dedup,
  file.path(output_dir, "umgc_attempt_count_audit_after_dedup.csv"),
  row.names = FALSE
)

# THEN retain the first 18 real rows
# The target retained form is a single 18-response reading administration,
# duplicate same-completion rows are removed first,
# then the earliest complete attempt is retained for comparability across administrations,
# later responses are not mixed into the retained attempt.

items_ua23_first18 <- retain_first_read_attempt(items_ua23_clean, 18L)
items_umgc_first18 <- retain_first_read_attempt(items_umgc_clean, 18L)

# audit retained 18 after deduplication
ua23_first18_audit <- items_ua23_first18 %>%
  group_by(global_id) %>%
  summarise(
    n_rows_first18 = n(),
    n_distinct_question_id = n_distinct(question_id),
    n_mapped_qid = sum(!is.na(QID)),
    n_distinct_qid = n_distinct(QID[!is.na(QID)]),
    n_unmapped_qid = sum(is.na(QID)),
    .groups = "drop"
  )

umgc_first18_audit <- items_umgc_first18 %>%
  group_by(global_id) %>%
  summarise(
    n_rows_first18 = n(),
    n_distinct_question_id = n_distinct(question_id),
    n_mapped_qid = sum(!is.na(QID)),
    n_distinct_qid = n_distinct(QID[!is.na(QID)]),
    n_unmapped_qid = sum(is.na(QID)),
    .groups = "drop"
  )

write.csv(
  ua23_first18_audit,
  file.path(output_dir, "ua23_first18_mapping_audit.csv"),
  row.names = FALSE
)

write.csv(
  umgc_first18_audit,
  file.path(output_dir, "umgc_first18_mapping_audit.csv"),
  row.names = FALSE
)

# strict keep rule: 18 retained rows, 18 distinct question_ids, 18 mapped QIDs
# this is a strict comparability rule:
# retained student records must represent one complete, uniquely identified 18-item administration
# partial, duplicated, or unmapped retained sets are excluded from the final calibrated dataset
ua23_mapped18_keep <- ua23_first18_audit %>%
  filter(
    n_rows_first18 == 18,
    n_distinct_question_id == 18,
    n_mapped_qid == 18
  ) %>%
  select(global_id)

umgc_mapped18_keep <- umgc_first18_audit %>%
  filter(
    n_rows_first18 == 18,
    n_distinct_question_id == 18,
    n_mapped_qid == 18
  ) %>%
  select(global_id)

# ============================================================
# Section 5. Student-level timing audit after cleaned first-18
# ============================================================

# this is a response-validity screen for implausibly rapid completion of the retained 18-item reading set
make_student_read_audit <- function(items_std, min_seconds = MIN_SECONDS_READ) {
  items_std %>%
    group_by(global_id) %>%
    summarise(
      DAACS_ID = first(DAACS_ID),
      college = first(college),
      wave = first(wave),
      n_answered = sum(!is.na(score)),
      n_distinct_question_id = n_distinct(question_id),
      readStartDate = suppressWarnings(min(startDate, na.rm = TRUE)),
      readCompletionDate = suppressWarnings(max(completion_dt, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      readStartDate = if_else(is.infinite(as.numeric(readStartDate)), as.POSIXct(NA), readStartDate),
      readCompletionDate = if_else(is.infinite(as.numeric(readCompletionDate)), as.POSIXct(NA), readCompletionDate),
      readTime = if_else(
        is.na(readStartDate) | is.na(readCompletionDate),
        NA_integer_,
        as.integer(difftime(readCompletionDate, readStartDate, units = "secs"))
      ),
      flag_not_exactly_18 = n_answered != 18L,
      flag_duplicate_questions = n_distinct_question_id != 18L,
      flag_speedy = !is.na(readTime) & n_answered == 18L & readTime < min_seconds,
      keep_student = !flag_not_exactly_18 & !flag_duplicate_questions & !flag_speedy
    )
}

ua23_read_audit <- make_student_read_audit(items_ua23_first18)
umgc_read_audit <- make_student_read_audit(items_umgc_first18)

write.csv(ua23_read_audit, file.path(output_dir, "ua23_read_student_audit.csv"), row.names = FALSE)
write.csv(umgc_read_audit, file.path(output_dir, "umgc_read_student_audit.csv"), row.names = FALSE)

# ============================================================
# Section 6. Keep retained students and join timing back
# ============================================================

#long files preserve item-level response history for audits and future analyses
read_items_ua23_long <- items_ua23_first18 %>%
  semi_join(
    ua23_read_audit %>% filter(keep_student) %>% select(global_id),
    by = "global_id"
  ) %>%
  semi_join(ua23_mapped18_keep, by = "global_id")

read_items_umgc_long <- items_umgc_first18 %>%
  semi_join(
    umgc_read_audit %>% filter(keep_student) %>% select(global_id),
    by = "global_id"
  ) %>%
  semi_join(umgc_mapped18_keep, by = "global_id")

institution_ua23_std <- institution_ua23_std %>%
  semi_join(
    ua23_read_audit %>% filter(keep_student) %>% select(global_id),
    by = "global_id"
  ) %>%
  semi_join(ua23_mapped18_keep, by = "global_id") %>%
  left_join(
    ua23_read_audit %>%
      filter(keep_student) %>%
      select(global_id, readCompletionDate, readTime),
    by = "global_id"
  )

institution_umgc_std <- institution_umgc_std %>%
  semi_join(
    umgc_read_audit %>% filter(keep_student) %>% select(global_id),
    by = "global_id"
  ) %>%
  semi_join(umgc_mapped18_keep, by = "global_id") %>%
  left_join(
    umgc_read_audit %>%
      filter(keep_student) %>%
      select(global_id, readCompletionDate, readTime),
    by = "global_id"
  )

ua23_time_summary <- qa_check_read_time_seconds(institution_ua23_std, "institution_ua23_std")
umgc_time_summary <- qa_check_read_time_seconds(institution_umgc_std, "institution_umgc_std")

write.csv(ua23_time_summary, file.path(output_dir, "ua23_read_time_summary.csv"), row.names = FALSE)
write.csv(umgc_time_summary, file.path(output_dir, "umgc_read_time_summary.csv"), row.names = FALSE)

# ============================================================
# Section 7. Build wide student-level reading files
# ============================================================

# wide files are the analysis-ready student-level inputs for downstream modeling

select_std_student_block <- function(df) {
  df %>%
    select(
      DAACS_ID, global_id, college, wave,
      age, age_d24, gender, ethnicity, military, pell, transfer,
      treat, fye_course, fye_semester,
      any_of(c("readCompletionDate", "readTime"))
    ) %>%
    distinct(global_id, .keep_all = TRUE)
}

make_read_wide <- function(items_std, institution_std) {
  item_wide <- items_std %>%
    filter(!is.na(QID)) %>%
    select(global_id, QID, score) %>%
    distinct(global_id, QID, .keep_all = TRUE) %>%
    pivot_wider(names_from = QID, values_from = score)
  
  qcols <- order_qid_cols(names(item_wide))
  
  item_wide <- item_wide %>%
    select(global_id, all_of(qcols))
  
  institution_std_keep <- institution_std %>%
    semi_join(items_std %>% distinct(global_id), by = "global_id")
  
  select_std_student_block(institution_std_keep) %>%
    left_join(item_wide, by = "global_id")
}

read_ua23_wide <- make_read_wide(read_items_ua23_long, institution_ua23_std)
read_umgc_wide <- make_read_wide(read_items_umgc_long, institution_umgc_std)

read_ua23umgc_wide <- bind_rows(
  read_ua23_wide,
  read_umgc_wide
) %>%
  distinct(global_id, .keep_all = TRUE)

# ============================================================
# Section 8. Wide-file QA
# ============================================================

qa_read_ua23 <- run_qa_wide_read(
  df = read_ua23_wide,
  dataset_name = "read_ua23_wide",
  output_dir = file.path(qa_dir, "qa_read_ua23")
)

qa_read_umgc <- run_qa_wide_read(
  df = read_umgc_wide,
  dataset_name = "read_umgc_wide",
  output_dir = file.path(qa_dir, "qa_read_umgc")
)

qa_read_ua23umgc <- run_qa_wide_read(
  df = read_ua23umgc_wide,
  dataset_name = "read_ua23umgc_wide",
  output_dir = file.path(qa_dir, "qa_read_ua23umgc")
)

# ============================================================
# Section 9. Final checks and QA summary
# ============================================================

cat("\nMapped QID completeness after first-18 retention:\n")
cat("UA23 students with all 18 mapped: ",
    nrow(ua23_mapped18_keep), " of ", 
    n_distinct(items_ua23_first18$global_id), "\n")
cat("UMGC students with all 18 mapped: ",
    nrow(umgc_mapped18_keep), " of ", 
    n_distinct(items_umgc_first18$global_id), "\n")

stopifnot(sum(duplicated(read_ua23_wide$global_id)) == 0)
stopifnot(sum(duplicated(read_umgc_wide$global_id)) == 0)
stopifnot(sum(duplicated(read_ua23umgc_wide$global_id)) == 0)

stopifnot(all(count_answered_items(read_ua23_wide) == 18))
stopifnot(all(count_answered_items(read_umgc_wide) == 18))
stopifnot(all(count_answered_items(read_ua23umgc_wide) == 18))

qa_read_summary <- list(
  institution_ua23_n_raw = nrow(institution.ua23.treat),
  institution_umgc_n_raw = nrow(institution.umgc.treat),
  
  institution_ua23_n_retained = nrow(institution_ua23_std),
  institution_umgc_n_retained = nrow(institution_umgc_std),
  
  read_items_ua23_long_n = nrow(read_items_ua23_long),
  read_items_umgc_long_n = nrow(read_items_umgc_long),
  
  ua23_unique_question_ids = n_distinct(read_items_ua23_long$question_id),
  umgc_unique_question_ids = n_distinct(read_items_umgc_long$question_id),
  
  ua23_unique_qids = n_distinct(read_items_ua23_long$QID, na.rm = TRUE),
  umgc_unique_qids = n_distinct(read_items_umgc_long$QID, na.rm = TRUE),
  
  ua23_missing_qid_rows = sum(is.na(read_items_ua23_long$QID)),
  umgc_missing_qid_rows = sum(is.na(read_items_umgc_long$QID)),
  
  ua23_not_exactly_18_excluded = sum(ua23_read_audit$flag_not_exactly_18, na.rm = TRUE),
  umgc_not_exactly_18_excluded = sum(umgc_read_audit$flag_not_exactly_18, na.rm = TRUE),
  
  ua23_speedy_excluded = sum(ua23_read_audit$flag_speedy, na.rm = TRUE),
  umgc_speedy_excluded = sum(umgc_read_audit$flag_speedy, na.rm = TRUE),
  
  ua23_duplicate_question_excluded = sum(ua23_read_audit$flag_duplicate_questions, na.rm = TRUE),
  umgc_duplicate_question_excluded = sum(umgc_read_audit$flag_duplicate_questions, na.rm = TRUE),
  
  read_ua23_wide_n = nrow(read_ua23_wide),
  read_umgc_wide_n = nrow(read_umgc_wide),
  read_ua23umgc_wide_n = nrow(read_ua23umgc_wide)
)

print(qa_read_summary)

ua23_duplicate_item_keys <- read_items_ua23_long %>%
  count(global_id, question_id) %>%
  filter(n > 1)

umgc_duplicate_item_keys <- read_items_umgc_long %>%
  count(global_id, question_id) %>%
  filter(n > 1)

# ============================================================
# Section 10. Save outputs
# ============================================================

save_both(institution_ua23_std, output_dir, "institution_ua23_std")
save_both(institution_umgc_std, output_dir, "institution_umgc_std")

save_both(read_items_ua23_long, output_dir, "read_items_ua23_long")
save_both(read_items_umgc_long, output_dir, "read_items_umgc_long")

save_both(read_ua23_wide, output_dir, "read_ua23_wide")
save_both(read_umgc_wide, output_dir, "read_umgc_wide")
save_both(read_ua23umgc_wide, output_dir, "read_ua23umgc_wide")

saveRDS(qid_lookup_obj$template_qid, file.path(output_dir, 
                                              "template_qid_read_ua23umgc.rds"))
saveRDS(qa_read_summary, file.path(output_dir, "qa_read_summary.rds"))

safe_write_csv(
  tibble(metric = names(qa_read_summary), value = unlist(qa_read_summary)),
  file.path(output_dir, "qa_summary.csv"),
  row.names = FALSE
)

safe_write_csv(
  ua23_duplicate_item_keys,
  file.path(output_dir, "ua23_duplicate_item_keys.csv"),
  row.names = FALSE
)

safe_write_csv(
  umgc_duplicate_item_keys,
  file.path(output_dir, "umgc_duplicate_item_keys.csv"),
  row.names = FALSE
)

cat("\nSaved reading 2022-23 outputs to:\n", output_dir, "\n")
cat("\nRows:\n")
cat("UA23 wide: ", nrow(read_ua23_wide), "\n")
cat("UMGC wide: ", nrow(read_umgc_wide), "\n")
cat("Combined wide: ", nrow(read_ua23umgc_wide), "\n")