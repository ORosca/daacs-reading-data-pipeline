# read_v2_umgc_ua_22_23_02_combined_qa_duplicate_diagnostics.R
# ============================================================
# Detect duplicate students and duplicate score patterns in the
# combined stacked reading dataset.
# This pipeline 
# 1. distinguishes duplicate records from likely repeat representations of the 
# same student across source files
# 2. prepares auditable artifacts for patch-then-remove deduplication in the 
# finalization script
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(tibble)
})

project_dir <- "C:/Users/orosc/OneDrive/Read"
source(file.path(project_dir, "utils_read_pipeline.R"))
output_dir <- file.path(project_dir, "read_v2_umgc_ua_22_23_combined_qa_outputs")

input_file <- file.path(output_dir, "read_v2_umgc_ua_2waves4sets_raw.rds")
stopifnot(file.exists(input_file))

dat_check <- readRDS(input_file)

# ------------------------------------------------------------
# Section 1. Duplicate checks
# order:
# duplicate global_id
# full-row duplicates
# substantive duplicates excluding ID-like fields
# score-only duplicates
# DAACS_ID duplicates within college
# ------------------------------------------------------------

dup_global_id <- dat_check %>%
  count(global_id, sort = TRUE) %>%
  filter(n > 1)

dup_global_id_rows <- dat_check %>%
  semi_join(dup_global_id, by = "global_id") %>%
  arrange(global_id, DAACS_ID, college, wave)

dup_full_row_flag <- duplicated(dat_check) | duplicated(dat_check, fromLast = TRUE)
dup_full_rows <- dat_check[dup_full_row_flag, , drop = FALSE] %>%
  arrange(DAACS_ID, college, wave)

id_like_cols <- intersect(c("DAACS_ID", "global_id", "college", "wave"), names(dat_check))
substantive_cols <- setdiff(names(dat_check), id_like_cols)

dup_substantive_rows <- dat_check %>%
  mutate(.row_index = row_number()) %>%
  group_by(across(all_of(substantive_cols))) %>%
  mutate(substantive_dup_n = n()) %>%
  ungroup() %>%
  filter(substantive_dup_n > 1) %>%
  arrange(DAACS_ID, college, wave)

score_cols <- names(dat_check)[grepl("^Q\\d{3}", names(dat_check))]

dup_score_only_rows <- dat_check %>%
  mutate(.row_index = row_number()) %>%
  group_by(across(all_of(score_cols))) %>%
  mutate(score_only_dup_n = n()) %>%
  ungroup() %>%
  filter(score_only_dup_n > 1) %>%
  arrange(DAACS_ID, college, wave)

score_cols_dup <- names(dup_score_only_rows)[grepl("^Q\\d{3}", names(dup_score_only_rows))]

# identical score patterns do not automatically imply the same person
# they are used as candidate duplicate groups for further demographic/profile checks
dup_score_only_rows <- dup_score_only_rows %>%
  mutate(
    score_pattern = apply(select(., all_of(score_cols_dup)), 1, function(x) {
      paste(ifelse(is.na(x), "NA", x), collapse = "|")
    })
  ) %>%
  group_by(score_pattern) %>%
  mutate(
    dup_group = cur_group_id(),
    n_in_group = n()
  ) %>%
  ungroup() %>%
  arrange(dup_group, DAACS_ID, global_id)

dup_daacs_within_college <- dat_check %>%
  count(college, DAACS_ID, sort = TRUE) %>%
  filter(n > 1)

dup_daacs_within_college_rows <- dat_check %>%
  semi_join(dup_daacs_within_college, by = c("college", "DAACS_ID")) %>%
  arrange(college, DAACS_ID, wave)

# ------------------------------------------------------------
# Section 2. Determine whether duplicate score patterns are
# likely the same person
# ------------------------------------------------------------

demo_check_vars <- c("wave", "age", "gender", "ethnicity")
assert_has_cols(
  dup_score_only_rows,
  c("dup_group", "DAACS_ID", "global_id", demo_check_vars),
  "dup_score_only_rows"
)

# this is a heuristic screen, not proof of identity
# groups are treated as likely cross-source representations only when score pattern and key demographics align
# the later patch rule is restricted to a narrow UMGC/UMGC1 case
dup_score_demo_summary <- dup_score_only_rows %>%
  group_by(dup_group) %>%
  summarise(
    n_rows = n(),
    n_global_id = n_distinct(global_id),
    n_DAACS_ID = n_distinct(DAACS_ID),
    n_wave = n_distinct(wave),
    n_age = n_distinct(age),
    n_gender = n_distinct(gender),
    n_ethnicity = n_distinct(ethnicity),
    same_demo_profile = n_wave == 1 & n_age == 1 & n_gender == 1 & n_ethnicity == 1,
    same_global_id = n_global_id == 1,
    same_DAACS_ID = n_DAACS_ID == 1,
    likely_same_person = same_demo_profile & same_global_id & same_DAACS_ID,
    .groups = "drop"
  ) %>%
  arrange(desc(!likely_same_person), dup_group)

# ------------------------------------------------------------
# Section 3. Select rows to remove and create patched UMGC rows
# Rule:
# only duplicate-score groups with aligned demographics are considered for patching
# only one-to-one umgc/umgc1 pairs are patched
# the umgc row is retained as the surviving record
# missing values in the retained umgc row are filled from the matched umgc1 row
# the matched umgc1 row is then removed in finalization
# ------------------------------------------------------------

# UMGC is retained as the surviving row because it is the later cleaned version 
# of the same source data. The earlier UMGC1 file, received in September 2023, 
# was a raw delivery with a technical error that left demographic information 
# available for only about 25% of students. The later UMGC file reflects 
# substantial checking and revision, with demographic coverage of about 100%. 
# Therefore, patching is one-way: missing selected values in the retained UMGC 
# row may be filled from the matched UMGC1 row, but when values conflict, the 
# UMGC value is kept.

# 3A. Keep only duplicate groups that are true umgc/umgc1 pairs
groups_same_demo_pairs <- dup_score_only_rows %>%
  semi_join(
    dup_score_demo_summary %>% filter(same_demo_profile),
    by = "dup_group"
  ) %>%
  count(dup_group, college, name = "n_by_college") %>%
  tidyr::pivot_wider(
    names_from = college,
    values_from = n_by_college,
    values_fill = 0
  ) %>%
  filter(umgc == 1, umgc1 == 1) %>%
  select(dup_group)

# 3B. Columns that may be patched from umgc1 into umgc

# Keep UMGC identifiers and provenance unchanged.
# identifiers and provenance fields are intentionally not overwritten during patching
# patching is restricted to analytic/content fields with missing values
cols_not_to_patch <- c(
  "DAACS_ID", "global_id", "college", "source_file",
  "dup_group", "n_in_group", "score_pattern", ".row_index"
)

patch_cols <- setdiff(names(dup_score_only_rows), cols_not_to_patch)

# 3C. Build one patched UMGC row per duplicate pair
umgc_rows_patched <- dup_score_only_rows %>%
  semi_join(groups_same_demo_pairs, by = "dup_group") %>%
  arrange(dup_group, college) %>%
  group_by(dup_group) %>%
  group_modify(~{
    umgc_row  <- .x %>% filter(college == "umgc")
    umgc1_row <- .x %>% filter(college == "umgc1")
    
    stopifnot(nrow(umgc_row) == 1, nrow(umgc1_row) == 1)
    
    patched <- umgc_row
    
# patching uses one-way coalescing only
# existing UMGC non-missing values are preserved
# UMGC1 contributes only where the UMGC retained row is missing
    
    for (nm in patch_cols) {
      patched[[nm]] <- dplyr::coalesce(umgc_row[[nm]], umgc1_row[[nm]])
    }
    
    patched
  }) %>%
  ungroup()

# 3D. Mark the umgc1 rows for removal
# these are not all duplicates in the data
# these are only the source rows selected for removal after successful patch preparation
rows_to_remove <- dup_score_only_rows %>%
  semi_join(groups_same_demo_pairs, by = "dup_group") %>%
  filter(college == "umgc1") %>%
  distinct(global_id)

# 3E. Optional QA: show how many values were filled into UMGC rows
# this is an audit table showing exactly which fields were filled in the retained UMGC rows.
patch_audit <- umgc_rows_patched %>%
  select(global_id, all_of(patch_cols)) %>%
  rename_with(~ paste0(.x, "_patched"), all_of(patch_cols)) %>%
  left_join(
    dup_score_only_rows %>%
      semi_join(groups_same_demo_pairs, by = "dup_group") %>%
      filter(college == "umgc") %>%
      select(global_id, all_of(patch_cols)),
    by = "global_id"
  )

patch_audit$n_fields_filled <- rowSums(
  sapply(patch_cols, function(nm) {
    is.na(patch_audit[[nm]]) & !is.na(patch_audit[[paste0(nm, "_patched")]])
  })
)

patch_audit <- patch_audit %>%
  arrange(desc(n_fields_filled), global_id)

patch_audit$fields_filled <- sapply(seq_len(nrow(patch_audit)), function(i) {
  filled_now <- patch_cols[
    sapply(patch_cols, function(nm) {
      is.na(patch_audit[[nm]][i]) & !is.na(patch_audit[[paste0(nm, "_patched")]][i])
    })
  ]
  paste(filled_now, collapse = ", ")
})

patch_audit_small <- patch_audit %>%
  select(global_id, n_fields_filled, fields_filled)

# Optional quick checks
nrow(groups_same_demo_pairs)
nrow(rows_to_remove)
summary(patch_audit$n_fields_filled)
View(patch_audit)
View(patch_audit_small)

# ------------------------------------------------------------
# Section 4. Save outputs
# ------------------------------------------------------------

save_both(dup_global_id, output_dir, "dup_global_id")
save_both(dup_global_id_rows, output_dir, "dup_global_id_rows")
save_both(dup_full_rows, output_dir, "dup_full_rows")
save_both(dup_substantive_rows, output_dir, "dup_substantive_rows")
save_both(dup_score_only_rows, output_dir, "dup_score_only_rows")
save_both(dup_daacs_within_college, output_dir, "dup_daacs_within_college")
save_both(dup_daacs_within_college_rows, output_dir, "dup_daacs_within_college_rows")
save_both(dup_score_demo_summary, output_dir, "dup_score_demo_summary")
save_both(rows_to_remove, output_dir, "rows_to_remove")
save_both(umgc_rows_patched, output_dir, "umgc_rows_patched")
save_both(patch_audit, output_dir, "patch_audit")

qa_duplicates <- list(
  duplicate_global_id_n = nrow(dup_global_id),
  duplicate_full_row_n = nrow(dup_full_rows),
  duplicate_substantive_row_n = nrow(dup_substantive_rows),
  duplicate_score_only_row_n = nrow(dup_score_only_rows),
  duplicate_daacs_within_college_n = nrow(dup_daacs_within_college),
  rows_to_remove_n = nrow(rows_to_remove)
)

write.csv(
  tibble(metric = names(qa_duplicates), value = unlist(qa_duplicates)),
  file.path(output_dir, "duplicate_diagnostics_summary.csv"),
  row.names = FALSE
)
