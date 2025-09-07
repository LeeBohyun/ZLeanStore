library(ggplot2)
library(dplyr)
library(tidyr)

# Load CSV
df <- read.csv("trx.csv")

# Compute per-transaction flash writes (bytes)
df <- df %>%
  mutate(
    x_accum_gb_user = cumsum(ewmb_s + cpwmb_s) / 1024,  # cumulative USER writes in GB
    flash_diff = c(NA, diff(cum_flash_wmb)),
    ssd_bytes_per_txn = (flash_diff * 1024^2) / tps,
    user_bytes_per_txn = ((ewmb_s + cpwmb_s) * 1024^2) / tps,
    dwb_bytes_per_txn  = (dwbw_mb * 1024^2) / tps,
    flash_bytes_per_txn  = ssd_bytes_per_txn - user_bytes_per_txn - dwb_bytes_per_txn
  ) %>%
  mutate(flash_bytes_per_txn = pmax(flash_bytes_per_txn, 0))  # ensure non-negative

# Subset from TPS peak
start_row <- which.max(df$tps)
df_plot <- df[start_row:nrow(df), ]

# Aggregate per 100GB of cumulative USER write
# Aggregate per 10GB of cumulative USER write using maximum
bin_size_gb <- 10
df_binned <- df_plot %>%
  mutate(bin = floor(x_accum_gb_user / bin_size_gb) * bin_size_gb) %>%
  group_by(bin) %>%
  summarise(
    user_bytes_per_txn = max(user_bytes_per_txn, na.rm = TRUE),
    dwb_bytes_per_txn  = max(dwb_bytes_per_txn, na.rm = TRUE),
    flash_bytes_per_txn = max(flash_bytes_per_txn, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    bottom_user = 0,
    top_user = user_bytes_per_txn,
    bottom_dwb = top_user,
    top_dwb = bottom_dwb + dwb_bytes_per_txn,
    bottom_flash = top_dwb,
    top_flash = bottom_flash + flash_bytes_per_txn
  )


# Convert to long format for stacked ribbon
df_long <- df_binned %>%
  select(bin, bottom_user, top_user, bottom_dwb, top_dwb, bottom_flash, top_flash) %>%
  pivot_longer(
    cols = -bin,
    names_to = c(".value", "type"),
    names_pattern = "(bottom|top)_(.*)"
  )

df_long$type <- factor(df_long$type, levels = c("user", "dwb", "flash"))

# Plot stacked ribbons with smoothing
ggplot(df_long, aes(x = bin, fill = type)) +
  geom_ribbon(aes(ymin = bottom, ymax = top, color = type), alpha = 0.7, show.legend = TRUE) +
  geom_smooth(aes(y = top), method = "loess", se = FALSE, color = NA, span = 0.5) +
  scale_fill_manual(values = c("user" = "green", "dwb" = "orange", "flash" = "red"),
                    labels = c("User writes", "DWB writes", "Flash writes")) +
  scale_color_manual(values = c("user" = "green", "dwb" = "orange", "flash" = "red"), guide = "none") +
  theme_minimal(base_size = 14) +
  labs(
    x = "Cumulative USER Write (GB, binned every 100GB)",
    y = "Flash writes per transaction (bytes)",
    fill = "Write Type",
    title = "Smoothed Flash Writes Breakdown per 100GB of USER Writes"
  )
