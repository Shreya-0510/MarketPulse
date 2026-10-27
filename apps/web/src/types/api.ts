// Mirrors services/api/app/schemas.py. Datetimes arrive as ISO-8601 strings (UTC).
export type ISO = string;
export type Sentiment = "positive" | "neutral" | "negative";

export interface Bar { event_id: string; symbol: string; entity_id: string; interval: string; event_time: ISO;
  open: number | null; high: number | null; low: number | null; close: number; adjusted_close: number | null;
  volume: number | null; currency: string; data_mode: string; source: string; ingested_at: ISO; processed_at: ISO | null }
export interface HistoryResponse { symbol: string; entity_id: string; currency: string; interval: string;
  status: "OK" | "NO_DATA"; count: number; total_in_range: number; truncated: boolean; bars: Bar[]; note: string | null }
export interface LatestBar { close: number; bar_time: ISO; interval: string; data_mode: string; is_provisional: boolean }
export interface WatchlistItem { entity_id: string; symbol: string; display_name: string; market: string; exchange: string | null;
  instrument_type: string; currency: string; timezone: string; status: "OK" | "STALE" | "NO_DATA"; latest: LatestBar | null;
  previous_close: number | null; change_abs: number | null; change_pct: number | null; change_basis: string;
  bars_available: number; data_age_hours: number | null }
export interface WatchlistResponse { generated_at: ISO; stale_after_days: number; items: WatchlistItem[] }

export interface PipelineStatus { checked_at: ISO; mongodb: string; kafka: string; components: Record<string, any>[] }
export interface SourceStatus { name: string; type: string; url: string; enabled: boolean; implemented: boolean; state: string;
  verification: string; last_attempt_at: ISO | null; last_success_at: ISO | null; last_error: string | null;
  updated_at: ISO | null; totals: Record<string, number> }

interface TextItem { event_id: string; source: string; data_mode: string; entity_ids: string[]; sentiment_label: Sentiment | null;
  sentiment_score: number | null; event_time: ISO | null; ingested_at: ISO; processed_at: ISO | null;
  source_metadata: Record<string, string> }
export interface SocialPost extends TextItem { platform_post_id: string; author_id: string; text: string; created_at: ISO;
  language: string | null; relevance_score: number | null; post_url: string | null }
export interface NewsArticle extends TextItem { publisher: string; publisher_verified: boolean; title: string; summary: string | null;
  url: string | null; published_at: ISO | null; timestamp_quality: "source" | "missing" }
interface Page { status: "OK" | "NO_DATA"; count: number; total: number; limit: number; offset: number; note: string | null }
export interface SocialResponse extends Page { items: SocialPost[] }
export interface NewsResponse extends Page { items: NewsArticle[] }

export interface SignalStats { count: number; scored: number; positive: number; neutral: number; negative: number; avg_score: number | null }
export interface EntityMetrics { entity_id: string; metric: string; window_start: ISO; window_end: ISO; window_minutes: number;
  social: SignalStats; news: SignalStats; total_mentions: number; computed_at: ISO }
export interface EntityMetricsResponse { entity_id: string; display_name: string; symbol: string; market: string; currency: string;
  status: "OK" | "NO_DATA"; windows: EntityMetrics[]; note: string | null }
export interface AttentionMetric { entity_id: string; source_type: "social" | "news" | "market"; window_start: ISO; window_end: ISO;
  window_minutes: number; count: number; computed_at: ISO }
export interface AttentionResponse { status: "OK" | "NO_DATA"; count: number; items: AttentionMetric[]; note: string | null }
export interface AnomalyEvent { anomaly_id: string; entity_id: string; source_type: string; window_start: ISO; window_end: ISO;
  window_minutes: number; observed_count: number; baseline_mean: number; baseline_std: number; baseline_windows_used: number;
  z_score: number; z_threshold: number; direction: "spike" | "drop"; detected_at: ISO }
export interface AnomalyResponse { status: "OK" | "NO_DATA"; count: number; items: AnomalyEvent[]; note: string | null }
export interface CrossSignal { entity_id: string; window_start: ISO; window_end: ISO; window_minutes: number; price_change_pct: number | null;
  price_bar_count: number; social_count: number; social_scored_count: number; avg_social_sentiment: number | null; news_count: number;
  news_scored_count: number; avg_news_sentiment: number | null; observations: string[]; note: string; computed_at: ISO }
export interface CrossSignalResponse { status: "OK" | "NO_DATA"; count: number; items: CrossSignal[]; note: string | null }
export interface EntitySummary { entity_id: string; display_name: string; symbol: string; market: string; currency: string; instrument_type: string }
export interface EntitiesResponse { count: number; items: EntitySummary[] }

export type WSType = "social_post" | "news_article" | "anomaly" | "source_status" | "ping" | "error";
export interface WSMessage { type: WSType; payload: any; sent_at: ISO }
