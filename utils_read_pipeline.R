# utils_read_pipeline.R
# ============================================================
# Shared utility functions for DAACS read QA pipelines
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(tidyr)
  library(stringr)
  library(tibble)
  library(purrr)
  library(ggplot2)
})

# ------------------------------------------------------------
# Core helpers
# ------------------------------------------------------------

`%||%` <- function(a, b) if (!is.null(a)) a else b

safe_write_csv <- function(df, path, row.names = FALSE) {
  if (file.exists(path)) {
    try(unlink(path), silent = TRUE)
  }
  write.csv(df, path, row.names = row.names)
}

assert_has_cols <- function(df, cols, df_name = "data") {
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) {
    stop(
      sprintf("[%s] missing required columns: %s", df_name, paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

save_both <- function(df, out_dir, file_stem) {
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  saveRDS(df, file.path(out_dir, paste0(file_stem, ".rds")))
  write.csv(
    df,
    file.path(out_dir, paste0(file_stem, ".csv")),
    row.names = FALSE,
    na = ""
  )
}

load_wide_input <- function(input_rds, input_csv = NULL) {
  if (file.exists(input_rds)) {
    readRDS(input_rds)
  } else if (!is.null(input_csv) && file.exists(input_csv)) {
    read_csv(input_csv, show_col_types = FALSE)
  } else {
    stop("No input file found. Check input_rds / input_csv paths.", call. = FALSE)
  }
}

normalize_daacs_id <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x <- sub("\\.0+$", "", x)
  x
}

make_global_id <- function(college_id, wave, daacs_id_raw) {
  paste(college_id, as.character(wave), daacs_id_raw, sep = "_")
}

# ------------------------------------------------------------
# Item-column helpers
# ------------------------------------------------------------

get_item_cols <- function(df) {
  item_cols <- names(df)[grepl("^Q\\d{3}", names(df))]
  as.character(item_cols)
}

order_qid_cols <- function(x) {
  qcols <- x[grepl("^Q\\d{3}", x)]
  qcols[order(substr(qcols, 1, 4), qcols)]
}

reorder_item_columns <- function(df) {
  item_cols <- order_qid_cols(names(df))
  non_item_cols <- names(df)[!names(df) %in% item_cols]
  df %>% select(all_of(non_item_cols), all_of(item_cols))
}

coerce_factors_to_char <- function(df) {
  df %>% mutate(across(where(is.factor), as.character))
}

coerce_item_cols <- function(df) {
  item_cols <- get_item_cols(df)
  if (length(item_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(item_cols), ~ suppressWarnings(as.integer(.x))))
  }
  df
}

align_to_master_cols <- function(df, master_cols) {
  missing_cols <- setdiff(master_cols, names(df))
  if (length(missing_cols) > 0) {
    for (nm in missing_cols) {
      df[[nm]] <- NA
    }
  }
  df %>% select(all_of(master_cols))
}

count_answered_items <- function(df) {
  item_cols <- get_item_cols(df)
  rowSums(!is.na(df[item_cols]))
}

# ------------------------------------------------------------
# Shared recodes
# ------------------------------------------------------------

recode_ethnicity_common <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  
  case_when(
    is.na(x) | x == "" ~ NA_character_,
    str_detect(x, regex("^White", ignore_case = TRUE)) ~ "White",
    str_detect(x, regex("Asian", ignore_case = TRUE)) ~ "Asian",
    str_detect(x, regex("Black|African", ignore_case = TRUE)) ~ "Black",
    str_detect(x, regex("Hispanic|Latino", ignore_case = TRUE)) ~ "Hispanic",
    TRUE ~ "Other"
  )
}

recode_yes_no <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  
  case_when(
    x %in% c("Yes", "Y", "1", "TRUE", "True") ~ "Yes",
    x %in% c("No", "N", "0", "FALSE", "False") ~ "No",
    TRUE ~ NA_character_
  )
}

# ------------------------------------------------------------
# QID mapping helpers
# ------------------------------------------------------------

load_qid_mapping <- function(mapping_xlsx) {
  map <- readxl::read_excel(mapping_xlsx)
  
  if (all(c("qid_ua2", "QID") %in% names(map))) {
    map %>%
      transmute(
        qid_ua2 = as.character(qid_ua2),
        QID = as.character(QID)
      ) %>%
      distinct(qid_ua2, .keep_all = TRUE)
  } else if (all(c("question_id", "QID") %in% names(map))) {
    map %>%
      transmute(
        question_id = as.character(question_id),
        QID = as.character(QID),
        assigned_difficulty = if ("assigned_difficulty" %in% names(map)) as.character(assigned_difficulty) else NA_character_,
        map_domain = if ("domain" %in% names(map)) as.character(domain) else NA_character_,
        map_question = if ("question" %in% names(map)) as.character(question) else NA_character_
      ) %>%
      distinct(question_id, .keep_all = TRUE)
  } else {
    stop(
      "Mapping file must contain either (qid_ua2, QID) or (question_id, QID).",
      call. = FALSE
    )
  }
}

rename_item_columns_to_final_qid <- function(df, mapping_df) {
  item_cols <- get_item_cols(df)
  
  if (!all(c("qid_ua2", "QID") %in% names(mapping_df))) {
    stop("mapping_df must contain qid_ua2 and QID for item-column renaming.", call. = FALSE)
  }
  
  mapping_use <- mapping_df %>%
    filter(qid_ua2 %in% item_cols)
  
  rename_vec <- setNames(mapping_use$qid_ua2, mapping_use$QID)
  
  df %>% rename(!!!rename_vec)
}

load_llm_repair_table <- function(llm_patch_csv) {
  if (!file.exists(llm_patch_csv)) {
    return(NULL)
  }
  
  llm <- readr::read_csv(llm_patch_csv, show_col_types = FALSE)
  
  required <- c("source_name", "question_id", "approved_QID")
  missing <- setdiff(required, names(llm))
  if (length(missing) > 0) {
    stop(
      sprintf(
        "LLM patch file is missing required columns: %s",
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  
  llm %>%
    transmute(
      source_name = as.character(source_name),
      question_id = as.character(question_id),
      QID = as.character(approved_QID),
      repair_source = "llm_human_review"
    ) %>%
    distinct(source_name, question_id, .keep_all = TRUE)
}

# ------------------------------------------------------------
# Filtering helpers
# ------------------------------------------------------------

filter_min_items <- function(items_std, min_items = 18L) {
  keep_ids <- items_std %>%
    group_by(global_id) %>%
    summarise(
      n_answered = sum(!is.na(score)),
      .groups = "drop"
    ) %>%
    filter(n_answered >= min_items)
  
  items_std %>%
    semi_join(keep_ids, by = "global_id")
}

filter_speedy_read_students <- function(
    df,
    min_items = 18L,
    min_seconds = 210L   # 3.5 minutes
) {
  assert_has_cols(df, c("global_id", "readTime"), "read wide data")
  
  out <- df %>%
    mutate(
      n_answered = count_answered_items(.),
      readTime_num = suppressWarnings(as.numeric(readTime)),
      flagged_speedy = !is.na(readTime_num) &
        n_answered >= min_items &
        readTime_num < min_seconds
    )
  
  audit <- out %>%
    transmute(
      global_id,
      DAACS_ID = if ("DAACS_ID" %in% names(.)) DAACS_ID else NA_character_,
      college = if ("college" %in% names(.)) college else NA_character_,
      wave = if ("wave" %in% names(.)) wave else NA_integer_,
      n_answered,
      readTime = readTime_num,
      flagged_speedy
    )
  
  filtered_df <- out %>%
    filter(!flagged_speedy) %>%
    select(-n_answered, -readTime_num, -flagged_speedy)
  
  summary <- tibble::tibble(
    metric = c(
      "n_total",
      "n_with_nonmissing_time",
      "n_with_18plus_answered",
      "n_flagged_speedy",
      "n_retained"
    ),
    value = c(
      nrow(df),
      sum(!is.na(audit$readTime)),
      sum(audit$n_answered >= min_items, na.rm = TRUE),
      sum(audit$flagged_speedy, na.rm = TRUE),
      nrow(filtered_df)
    )
  )
  
  list(
    data = filtered_df,
    summary = summary,
    flagged_cases = audit %>% filter(flagged_speedy),
    full_audit = audit
  )
}

# ------------------------------------------------------------
# QA helpers
# ------------------------------------------------------------

qa_check_read_time_seconds <- function(df, df_name = "data", min_reasonable_seconds = 210L) {
  if (!"readTime" %in% names(df)) {
    message(sprintf("[%s] readTime column not found.", df_name))
    return(invisible(NULL))
  }
  
  x <- suppressWarnings(as.numeric(df$readTime))
  
  out <- tibble(
    dataset = df_name,
    n = sum(!is.na(x)),
    min = suppressWarnings(min(x, na.rm = TRUE)),
    q1 = suppressWarnings(as.numeric(quantile(x, 0.25, na.rm = TRUE))),
    median = suppressWarnings(median(x, na.rm = TRUE)),
    mean = suppressWarnings(mean(x, na.rm = TRUE)),
    q3 = suppressWarnings(as.numeric(quantile(x, 0.75, na.rm = TRUE))),
    max = suppressWarnings(max(x, na.rm = TRUE)),
    n_missing = sum(is.na(x)),
    n_nonpositive = sum(!is.na(x) & x <= 0),
    n_under_1_min = sum(!is.na(x) & x < 60),
    n_under_2_min = sum(!is.na(x) & x < 120),
    n_under_3_5_min = sum(!is.na(x) & x < min_reasonable_seconds),
    n_over_8_hours = sum(!is.na(x) & x > 8 * 60 * 60)
  )
  
  print(out)
  invisible(out)
}

qa_duplicates <- function(df) {
  dup_daacs <- unique(df$DAACS_ID[duplicated(df$DAACS_ID)])
  dup_global <- unique(df$global_id[duplicated(df$global_id)])
  
  list(
    summary = data.frame(
      metric = c("duplicate_DAACS_ID_n", "duplicate_global_id_n"),
      value = c(length(dup_daacs), length(dup_global))
    ),
    duplicate_values = data.frame(
      duplicate_DAACS_ID_values = c(dup_daacs, rep(NA, max(0, length(dup_global) - length(dup_daacs)))),
      duplicate_global_id_values = c(dup_global, rep(NA, max(0, length(dup_daacs) - length(dup_global))))
    )
  )
}

qa_identical_response_patterns <- function(df) {
  item_cols <- get_item_cols(df)
  mat <- df %>% select(all_of(item_cols)) %>% as.data.frame()
  
  dup_any <- duplicated(mat) | duplicated(mat, fromLast = TRUE)
  
  list(
    identical_patterns_n = sum(dup_any),
    identical_ids = df$global_id[dup_any]
  )
}

qa_non_discriminative_items <- function(df) {
  item_cols <- as.character(get_item_cols(df))
  
  if (length(item_cols) == 0) {
    return(character(0))
  }
  
  item_df <- df %>%
    dplyr::select(dplyr::all_of(item_cols)) %>%
    as.data.frame()
  
  keep <- vapply(item_df, function(x) {
    ux <- unique(x[!is.na(x)])
    length(ux) <= 1
  }, logical(1))
  
  item_cols[keep]
}

run_qa_wide_read <- function(df, dataset_name, output_dir) {
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  dupes <- qa_duplicates(df)
  ident <- qa_identical_response_patterns(df)
  nondisc <- qa_non_discriminative_items(df)
  ranges <- qa_value_ranges(df)
  item_table <- qa_item_counts(df)
  sample_size_report <- make_item_sample_size_report(df)
  read_time_summary <- qa_check_read_time_seconds(df, dataset_name)
  
  out <- list(
    dataset_name = dataset_name,
    n_students = nrow(df),
    n_items = length(get_item_cols(df)),
    duplicates = dupes,
    identical_patterns = ident,
    non_discriminative_items = nondisc,
    item_ranges = ranges,
    item_response_counts = item_table,
    sample_size_overall = sample_size_report$overall_counts,
    sample_size_by_demo = sample_size_report$by_demo_counts,
    read_time_summary = read_time_summary
  )
  
  saveRDS(out, file.path(output_dir, paste0(dataset_name, "_qa_results.rds")))
  safe_write_csv(ranges, file.path(output_dir, paste0(dataset_name, "_item_ranges.csv")), row.names = FALSE)
  safe_write_csv(item_table, file.path(output_dir, paste0(dataset_name, "_item_counts.csv")), row.names = FALSE)
  safe_write_csv(sample_size_report$overall_counts,
                 file.path(output_dir, paste0(dataset_name, "_sample_size_overall.csv")),
                 row.names = FALSE)
  safe_write_csv(sample_size_report$by_demo_counts,
                 file.path(output_dir, paste0(dataset_name, "_sample_size_by_demo.csv")),
                 row.names = FALSE)
  safe_write_csv(read_time_summary,
                 file.path(output_dir, paste0(dataset_name, "_read_time_summary.csv")),
                 row.names = FALSE)
  
  if ("plot_response_count_density" %in% ls()) {
    plot_response_count_density(
      item_table,
      out_file = file.path(output_dir, paste0(dataset_name, "_density_counts.png")),
      title = paste0(dataset_name, ": Distribution of Item Response Counts")
    )
  }
  
  out
}


qa_value_ranges <- function(df) {
  item_cols <- as.character(get_item_cols(df))
  
  if (length(item_cols) == 0) {
    return(tibble::tibble(QID = character(), min = numeric(), max = numeric(), n = integer()))
  }
  
  item_df <- df %>%
    dplyr::select(dplyr::all_of(item_cols)) %>%
    as.data.frame()
  
  tibble::tibble(
    QID = item_cols,
    min = vapply(item_df, function(x) suppressWarnings(min(x, na.rm = TRUE)), numeric(1)),
    max = vapply(item_df, function(x) suppressWarnings(max(x, na.rm = TRUE)), numeric(1)),
    n = vapply(item_df, function(x) sum(!is.na(x)), integer(1))
  ) %>%
    dplyr::arrange(QID)
}

qa_item_counts <- function(df) {
  item_cols <- as.character(get_item_cols(df))
  
  if (length(item_cols) == 0) {
    return(tibble::tibble(QID = character(), n_responded = integer()))
  }
  
  item_df <- df %>%
    dplyr::select(dplyr::all_of(item_cols)) %>%
    as.data.frame()
  
  tibble::tibble(
    QID = item_cols,
    n_responded = vapply(item_df, function(x) sum(!is.na(x)), integer(1))
  ) %>%
    dplyr::arrange(QID)
}

plot_response_count_density <- function(item_table, out_file, title = "Item response count distribution") {
  p <- ggplot(item_table, aes(x = n_responded)) +
    geom_density(adjust = 1.1, na.rm = TRUE) +
    labs(
      title = title,
      x = "Number of responses per item",
      y = "Density"
    ) +
    theme_minimal()
  
  ggsave(out_file, p, width = 8, height = 5, dpi = 300)
  invisible(p)
}

# ------------------------------------------------------------
# Item-level sample size summaries
# ------------------------------------------------------------

make_item_sample_size_report <- function(df) {
  item_cols <- as.character(get_item_cols(df))
  demo_vars <- c("age_d24", "gender", "ethnicity", "military", "pell")
  demo_vars <- intersect(demo_vars, names(df))
  
  if (length(item_cols) == 0) {
    return(list(
      overall_counts = tibble::tibble(
        QID = character(),
        n_responded = integer(),
        n_wrong = integer(),
        n_correct = integer()
      ),
      by_demo_counts = tibble::tibble(
        QID = character(),
        demographic = character(),
        group_value = character(),
        n_responded = integer(),
        n_wrong = integer(),
        n_correct = integer()
      )
    ))
  }
  
  long_df <- df %>%
    dplyr::select(global_id, dplyr::all_of(demo_vars), dplyr::all_of(item_cols)) %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(item_cols),
      names_to = "QID",
      values_to = "score"
    )
  
  overall_counts <- long_df %>%
    dplyr::filter(!is.na(score)) %>%
    dplyr::group_by(QID) %>%
    dplyr::summarise(
      n_responded = dplyr::n(),
      n_wrong = sum(score == 0, na.rm = TRUE),
      n_correct = sum(score == 1, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(QID)
  
  by_demo_counts <- purrr::map_dfr(demo_vars, function(v) {
    long_df %>%
      dplyr::filter(!is.na(score)) %>%
      dplyr::mutate(group_value = as.character(.data[[v]])) %>%
      dplyr::group_by(QID, demographic = v, group_value) %>%
      dplyr::summarise(
        n_responded = dplyr::n(),
        n_wrong = sum(score == 0, na.rm = TRUE),
        n_correct = sum(score == 1, na.rm = TRUE),
        .groups = "drop"
      )
  })
  
  list(
    overall_counts = overall_counts,
    by_demo_counts = by_demo_counts
  )
}

# ------------------------------------------------------------
# Missingness diagnostics
# ------------------------------------------------------------

run_missingness_diagnostics <- function(
    df,
    stage_name = "data",
    vars_miss = c("college", "wave", "age", "gender", "ethnicity", "military", "pell", "transfer"),
    group_vars = c("college", "wave"),
    save_plots = FALSE,
    output_dir = NULL,
    file_prefix = NULL
) {
  vars_miss <- intersect(vars_miss, names(df))
  group_vars <- intersect(group_vars, names(df))
  vars_to_summarise <- setdiff(vars_miss, group_vars)
  
  if (length(vars_miss) == 0) {
    stop("None of the requested vars_miss are present in df.")
  }
  
  if (isTRUE(save_plots) && is.null(output_dir)) {
    stop("If save_plots = TRUE, provide output_dir.")
  }
  
  if (is.null(file_prefix)) {
    file_prefix <- gsub("[^A-Za-z0-9_]+", "_", stage_name)
  }
  
  is_missing_mixed <- function(x) {
    x_chr <- trimws(as.character(x))
    is.na(x) | x_chr %in% c("", "NA", "<NA>", "NULL")
  }
  
  miss_table <- tibble(
    variable = vars_miss,
    n_missing = sapply(vars_miss, function(v) sum(is_missing_mixed(df[[v]])))
  )
  
  miss_df <- df %>%
    mutate(
      row_id = row_number(),
      across(all_of(vars_miss), as.character)
    ) %>%
    select(row_id, all_of(vars_miss)) %>%
    pivot_longer(
      cols = all_of(vars_miss),
      names_to = "variable",
      values_to = "value"
    ) %>%
    mutate(
      value = trimws(value),
      missing = is.na(value) | value %in% c("", "NA", "<NA>", "NULL")
    )
  
  miss_counts_long <- miss_df %>%
    count(variable, missing)
  
  row_order <- miss_df %>%
    group_by(row_id) %>%
    summarise(
      n_missing = sum(missing),
      wave_missing = any(variable == "wave" & missing),
      pell_missing = any(variable == "pell" & missing),
      gender_missing = any(variable == "gender" & missing),
      .groups = "drop"
    ) %>%
    arrange(desc(wave_missing), desc(pell_missing), desc(gender_missing), desc(n_missing), row_id) %>%
    mutate(row_order = row_number())
  
  miss_df_plot <- miss_df %>%
    left_join(row_order, by = "row_id") %>%
    group_by(row_id) %>%
    mutate(any_missing_row = any(missing)) %>%
    ungroup() %>%
    filter(any_missing_row) %>%
    mutate(row_order_f = factor(row_order, levels = rev(sort(unique(row_order)))))
  
  p_heatmap_rows <- ggplot(miss_df_plot, aes(x = variable, y = row_order_f, fill = missing)) +
    geom_raster() +
    scale_fill_manual(values = c("FALSE" = "grey85", "TRUE" = "black")) +
    labs(
      title = paste0("Missing-value heat map: ", stage_name),
      x = "Variable",
      y = "Ordered row",
      fill = "Missing"
    ) +
    theme_minimal() +
    theme(
      axis.text.y = element_blank(),
      axis.ticks.y = element_blank()
    )
  
  p_missing_only <- miss_df_plot %>%
    filter(missing) %>%
    ggplot(aes(x = variable, y = row_order_f)) +
    geom_point(shape = 15, size = 1.8, color = "black") +
    labs(
      title = paste0("Missing cells only: ", stage_name),
      x = "Variable",
      y = "Ordered row"
    ) +
    theme_minimal() +
    theme(
      axis.text.y = element_blank(),
      axis.ticks.y = element_blank()
    )
  
  if (length(group_vars) > 0 && length(vars_to_summarise) > 0) {
    miss_summary <- df %>%
      mutate(across(all_of(c(group_vars, vars_to_summarise)), as.character)) %>%
      group_by(across(all_of(group_vars))) %>%
      summarise(
        across(
          all_of(vars_to_summarise),
          ~ mean(is.na(.) | trimws(.) %in% c("", "NA", "<NA>", "NULL")),
          .names = "{.col}"
        ),
        .groups = "drop"
      ) %>%
      pivot_longer(
        cols = all_of(vars_to_summarise),
        names_to = "variable",
        values_to = "prop_missing"
      ) %>%
      mutate(group_label = as.character(interaction(!!!syms(group_vars), drop = TRUE)))
    
    p_grouped <- ggplot(miss_summary, aes(x = variable, y = group_label, fill = prop_missing)) +
      geom_tile() +
      labs(
        title = paste0("Proportion missing by group: ", stage_name),
        x = "Variable",
        y = paste(group_vars, collapse = " / "),
        fill = "Proportion missing"
      ) +
      theme_minimal()
  } else {
    miss_summary <- NULL
    p_grouped <- NULL
  }
  
  if (isTRUE(save_plots)) {
    dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
    
    ggsave(
      file.path(output_dir, paste0(file_prefix, "_missing_heatmap_rows.png")),
      p_heatmap_rows, width = 8, height = 10, dpi = 300
    )
    
    ggsave(
      file.path(output_dir, paste0(file_prefix, "_missing_cells_only.png")),
      p_missing_only, width = 8, height = 10, dpi = 300
    )
    
    if (!is.null(p_grouped)) {
      ggsave(
        file.path(output_dir, paste0(file_prefix, "_missing_grouped_heatmap.png")),
        p_grouped, width = 8, height = 5, dpi = 300
      )
    }
    
    write.csv(
      miss_table,
      file.path(output_dir, paste0(file_prefix, "_missing_table.csv")),
      row.names = FALSE
    )
  }
  
  invisible(list(
    miss_table = miss_table,
    miss_df = miss_df,
    miss_counts_long = miss_counts_long,
    row_order = row_order,
    miss_df_plot = miss_df_plot,
    miss_summary = miss_summary,
    p_heatmap_rows = p_heatmap_rows,
    p_missing_only = p_missing_only,
    p_grouped = p_grouped
  ))
}