read_v2_umgc_ua_22_23_02_combined_llm_harmonization.R
# ============================================================
# LLM-assisted harmonization audit module
# ============================================================
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tidyr)
library(purrr)
library(stringdist)

output_dir <- "C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_02
_llm_audit_outputs"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# --------------------------------------------------
# 1. Load files from existing harmonization pipeline
# --------------------------------------------------

template_qid_unmatched <- read_csv(
  "C:/Users/orosc/OneDrive/Read/read_v2_ua23umgc-2022-23_qa_outputs/template_qid_unmatched.csv",
  show_col_types = FALSE
)

template_qid_fuzzy_review <- read_csv(
  "C:/Users/orosc/OneDrive/Read/read_v2_ua23umgc-2022-23_qa_outputs/template_qid_fuzzy_review.csv",
  show_col_types = FALSE
)

template_qid_after_manual <- read_csv(
  "C:/Users/orosc/OneDrive/Read/read_v2_ua23umgc-2022-23_qa_outputs/template_qid_after_manual.csv",
  show_col_types = FALSE
)

# Mapping file really has only: stem, domain, QID
mapping_qid <- read_excel(
  "C:/Users/orosc/OneDrive/Read/MappingQID_read.xlsx"
) %>%
  transmute(
    stem = as.character(stem),
    domain = as.character(domain),
    QID = as.character(QID)
  ) %>%
  distinct(QID, .keep_all = TRUE)

# --------------------------------------------------
# 2. Define which rows go to LLM audit
# --------------------------------------------------

audit_unmatched <- template_qid_unmatched %>%
  mutate(audit_reason = "unmatched_after_exact_fuzzy")

audit_fuzzy <- template_qid_fuzzy_review %>%
  filter(!is.na(string_distance), string_distance >= 0.08) %>%
  mutate(audit_reason = "high_distance_fuzzy_match")

audit_manual <- template_qid_after_manual %>%
  filter(match_method == "manual_repair") %>%
  mutate(audit_reason = "manual_repair_secondary_check")

audit_cases <- bind_rows(audit_unmatched, audit_fuzzy, audit_manual) %>%
  distinct(source_name, question_id, .keep_all = TRUE)

# --------------------------------------------------
# 3. Keep only the fields that actually exist / matter
# --------------------------------------------------

llm_audit_input <- audit_cases %>%
  transmute(
    source_name = if ("source_name" %in% names(.)) as.character(source_name) else NA_character_,
    question_id = if ("question_id" %in% names(.)) as.character(question_id) else NA_character_,
    domainId = if ("domainId" %in% names(.)) as.character(domainId) else NA_character_,
    stem = if ("stem" %in% names(.)) as.character(stem) else NA_character_,
    current_QID = if ("QID" %in% names(.)) as.character(QID) else NA_character_,
    match_method = if ("match_method" %in% names(.)) as.character(match_method) else NA_character_,
    string_distance = if ("string_distance" %in% names(.)) as.numeric(string_distance) else NA_real_,
    audit_reason
  ) %>%
  mutate(
    llm_suggested_qid = NA_character_,
    llm_confidence = NA_character_,
    llm_rationale = NA_character_,
    human_final_decision = NA_character_,
    human_final_qid = NA_character_,
    audit_status = "pending"
  )

write_csv(llm_audit_input, file.path(output_dir, "llm_audit_input_read.csv"))

# --------------------------------------------------
# 4. Function to build candidate pool
# --------------------------------------------------
# Restrict by domain first if possible, then rank by stem similarity

build_candidate_pool <- function(domain_val, stem_val, item_bank, max_candidates = 5) {
  candidates <- item_bank
  
  if (!is.na(domain_val) && domain_val != "") {
    candidates <- candidates %>%
      filter(tolower(domain) == tolower(domain_val))
  }
  
  if (!is.na(stem_val) && stem_val != "") {
    candidates <- candidates %>%
      mutate(
        similarity_jw = stringsim(
          a = tolower(stem_val),
          b = tolower(stem),
          method = "jw"
        ),
        distance_lv = stringdist(
          a = tolower(stem_val),
          b = tolower(stem),
          method = "lv"
        )
      ) %>%
      arrange(desc(similarity_jw), distance_lv, QID)
  }
  
  candidates %>%
    slice_head(n = max_candidates)
}

# --------------------------------------------------
# 5. Actually generate candidate pools for all audit rows
# --------------------------------------------------

candidate_pool_list <- map(
  seq_len(nrow(llm_audit_input)),
  function(i) {
    row_i <- llm_audit_input[i, ]
    
    cand_i <- build_candidate_pool(
      domain_val = row_i$domainId,
      stem_val = row_i$stem,
      item_bank = mapping_qid,
      max_candidates = 5
    )
    
    if (nrow(cand_i) == 0) {
      return(
        tibble(
          audit_row = i,
          source_name = row_i$source_name,
          question_id = row_i$question_id,
          domainId = row_i$domainId,
          source_stem = row_i$stem,
          candidate_rank = NA_integer_,
          candidate_QID = NA_character_,
          candidate_domain = NA_character_,
          candidate_stem = NA_character_,
          similarity_jw = NA_real_,
          distance_lv = NA_real_
        )
      )
    }
    
    cand_i %>%
      mutate(
        audit_row = i,
        source_name = row_i$source_name,
        question_id = row_i$question_id,
        domainId = row_i$domainId,
        source_stem = row_i$stem,
        candidate_rank = row_number()
      ) %>%
      transmute(
        audit_row,
        source_name,
        question_id,
        domainId,
        source_stem,
        candidate_rank,
        candidate_QID = QID,
        candidate_domain = domain,
        candidate_stem = stem,
        similarity_jw,
        distance_lv
      )
  }
)

candidate_pool_df <- bind_rows(candidate_pool_list)

write_csv(candidate_pool_df, file.path(output_dir, "llm_candidate_pool_read.csv"))

# --------------------------------------------------
# 6. Create one prompt-ready table per audit row
# --------------------------------------------------

prompt_table <- candidate_pool_df %>%
  group_by(audit_row, source_name, question_id, domainId, source_stem) %>%
  summarise(
    candidates_text = paste0(
      candidate_rank, ". ",
      candidate_QID, " [", candidate_domain, "]: ",
      candidate_stem,
      collapse = "\n"
    ),
    .groups = "drop"
  ) %>%
  left_join(
    llm_audit_input %>%
      mutate(audit_row = row_number()) %>%
      select(audit_row, audit_reason, current_QID, match_method, string_distance),
    by = "audit_row"
  )

write_csv(prompt_table, file.path(output_dir, "llm_prompt_table_read.csv"))

# --------------------------------------------------
# 7. Create empty results shell for manual / LLM completion
# --------------------------------------------------

llm_audit_results <- prompt_table %>%
  mutate(
    llm_suggested_qid = NA_character_,
    llm_confidence = NA_character_,   # high / medium / low / no_reliable_match
    llm_rationale = NA_character_,
    human_final_decision = NA_character_,  # accept / reject / revise
    human_final_qid = NA_character_,
    audit_status = "pending"
  )

write_csv(llm_audit_results, file.path(output_dir, "llm_audit_results_read.csv"))

# --------------------------------------------------
# 8. Task to a human assistant
# --------------------------------------------------
# You are assisting with item-identity harmonization for a reading assessment.
# 
# Task:
#   Review the source item below and compare it to candidate QIDs.
# Do not assume a match if the evidence is weak.
# 
# Source item:
#   - source_name: {source_name}
# - question_id: {question_id}
# - domainId: {domainId}
# - stem: {source_stem}
# 
# Candidate QIDs:
#   {candidates_text}
# 
# Return llm_audit_results_read_completed.csv:
# human_final_qid
# human_final_decision = "reject" for unresolved

# --------------------------------------------------
# 9. Read completed human review file
# --------------------------------------------------

completed_review_file <- file.path(output_dir, "llm_audit_results_read_completed.csv")

if (!file.exists(completed_review_file)) {
  stop("Completed human review file not found: ", completed_review_file)
}

llm_audit_results_completed <- read_csv(
  completed_review_file,
  show_col_types = FALSE
)

# Basic required columns
required_cols <- c(
  "audit_row", "source_name", "question_id", "domainId", "source_stem",
  "human_final_qid"
)

missing_required <- setdiff(required_cols, names(llm_audit_results_completed))
if (length(missing_required) > 0) {
  stop("Completed review file is missing required columns: ",
       paste(missing_required, collapse = ", "))
}

# --------------------------------------------------
# 10. Clean and validate human decisions
# --------------------------------------------------

llm_audit_results_completed <- llm_audit_results_completed %>%
  mutate(
    human_final_decision = tolower(trimws(as.character(human_final_decision))),
    human_final_qid = trimws(as.character(human_final_qid)),
    human_final_qid = if_else(human_final_qid == "", NA_character_, human_final_qid),
    audit_status = if ("audit_status" %in% names(.)) {
      trimws(as.character(audit_status))
    } else {
      NA_character_
    }
  )

valid_decisions <- c("accept", "reject", "revise", NA_character_)
bad_decisions <- llm_audit_results_completed %>%
  filter(!(human_final_decision %in% valid_decisions))

if (nrow(bad_decisions) > 0) {
  stop("Invalid values found in human_final_decision. Allowed: accept, reject, revise, blank/NA.")
}

# QIDs accepted/revised must exist in the mapping file
valid_qids <- unique(mapping_qid$QID)

bad_qids <- llm_audit_results_completed %>%
  filter(
    human_final_decision %in% c("accept", "revise"),
    !is.na(human_final_qid),
    !(human_final_qid %in% valid_qids)
  )

if (nrow(bad_qids) > 0) {
  warning("Some accepted/revised QIDs are not found in MappingQID_read.xlsx.")
  write_csv(bad_qids, file.path(output_dir, "llm_bad_qids_for_review.csv"))
}

# Accepted/revised rows missing final QID
missing_final_qid <- llm_audit_results_completed %>%
  filter(
    human_final_decision %in% c("accept", "revise"),
    is.na(human_final_qid)
  )

if (nrow(missing_final_qid) > 0) {
  warning("Some accepted/revised rows are missing human_final_qid.")
  write_csv(missing_final_qid, file.path(output_dir, "llm_missing_final_qid_for_review.csv"))
}

# --------------------------------------------------
# 11. Create final approved LLM patch table
# --------------------------------------------------

llm_patch_final <- llm_audit_results_completed %>%
  filter(
    human_final_decision %in% c("accept", "revise"),
    !is.na(human_final_qid),
    human_final_qid %in% valid_qids
  ) %>%
  transmute(
    source_name,
    question_id,
    domainId,
    stem = source_stem,
    approved_QID = human_final_qid,
    approval_source = "llm_human_review",
    human_final_decision
  ) %>%
  distinct(source_name, question_id, .keep_all = TRUE)

write_csv(llm_patch_final, file.path(output_dir, "llm_patch_final_read.csv"))

# --------------------------------------------------
# 12. Audit summary tables
# --------------------------------------------------

llm_review_summary <- llm_audit_results_completed %>%
  count(human_final_decision, name = "n_rows")

write_csv(llm_review_summary, file.path(output_dir, "llm_review_summary_read.csv"))

llm_patch_summary <- tibble(
  n_total_reviewed = nrow(llm_audit_results_completed),
  n_accept = sum(llm_audit_results_completed$human_final_decision == "accept", na.rm = TRUE),
  n_reject = sum(llm_audit_results_completed$human_final_decision == "reject", na.rm = TRUE),
  n_revise = sum(llm_audit_results_completed$human_final_decision == "revise", na.rm = TRUE),
  n_final_patches = nrow(llm_patch_final)
)

write_csv(llm_patch_summary, file.path(output_dir, "llm_patch_summary_read.csv"))

# --------------------------------------------------
# 13. Apply approved patches back to audit cases
# --------------------------------------------------
# This does NOT overwrite your original files.
# It creates a harmonized version with LLM+human-approved QIDs attached.

audit_cases_with_llm <- audit_cases %>%
  left_join(
    llm_patch_final %>%
      select(source_name, question_id, approved_QID, approval_source, human_final_decision),
    by = c("source_name", "question_id")
  ) %>%
  mutate(
    final_QID = case_when(
      !is.na(approved_QID) ~ approved_QID,
      "QID" %in% names(.) & !is.na(QID) ~ as.character(QID),
      TRUE ~ NA_character_
    ),
    final_match_method = case_when(
      !is.na(approved_QID) ~ "llm_human_review",
      "match_method" %in% names(.) & !is.na(match_method) ~ as.character(match_method),
      TRUE ~ NA_character_
    )
  )

write_csv(audit_cases_with_llm, file.path(output_dir, "audit_cases_with_llm_read.csv"))

# --------------------------------------------------
# 14. Optional: create a ready-to-bind repair table
# --------------------------------------------------
# Use this if you want to append these approved mappings into a later
# manual-repair or final harmonization step.

llm_manual_repair_like <- llm_patch_final %>%
  transmute(
    source_name,
    question_id,
    domainId,
    stem,
    QID = approved_QID,
    match_method = "llm_human_review"
  )

write_csv(llm_manual_repair_like, file.path(output_dir, "llm_manual_repair_like_read.csv"))

# --------------------------------------------------
# 15. Optional: unresolved cases after human review
# --------------------------------------------------

llm_unresolved_cases <- llm_audit_results_completed %>%
  filter(
    human_final_decision == "reject" |
      is.na(human_final_decision) |
      (human_final_decision %in% c("accept", "revise") & is.na(human_final_qid))
  )

write_csv(llm_unresolved_cases, file.path(output_dir, "llm_unresolved_cases_read.csv"))

# --------------------------------------------------
# 16. Final message
# --------------------------------------------------

cat("\nLLM-assisted harmonization review integration finished.\n")
cat("Approved final patches:", nrow(llm_patch_final), "\n")
cat("Unresolved cases:", nrow(llm_unresolved_cases), "\n")