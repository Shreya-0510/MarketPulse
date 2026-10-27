import { ExternalLink } from "lucide-react";
import type { NewsArticle, SocialPost } from "../types/api";
import { fmtTime } from "../lib/format";
import { ExtLink, ModePill, SentimentTag } from "./ui";

export function HeadlineRow({ a, names }: { a: NewsArticle; names?: Record<string, string> }) {
  return (
    <li className="py-2">
      <div className="text-sm font-medium leading-snug"><ExtLink href={a.url}>{a.title}</ExtLink>{a.url && <ExternalLink className="ml-1 inline h-3 w-3 text-ink-300" />}</div>
      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-ink-500">
        <span>{a.publisher}{a.publisher_verified ? "" : " (inferred)"}</span>
        <span title="Publication time reported by the source">{a.published_at ? `published ${fmtTime(a.published_at)}` : "publication time missing"}</span>
        <SentimentTag label={a.sentiment_label} /><ModePill mode={a.data_mode} />
        {a.entity_ids.map((e) => <span key={e} className="rounded bg-brand-50 px-1.5 text-brand-700">{names?.[e] ?? e}</span>)}
      </div>
    </li>
  );
}

export function PostRow({ p, names }: { p: SocialPost; names?: Record<string, string> }) {
  return (
    <li className="py-2">
      <p className="line-clamp-3 break-words text-sm">{p.text}</p>
      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-ink-500">
        <span>{p.source}</span><span title="Author-claimed creation time">{fmtTime(p.created_at)}</span>
        <SentimentTag label={p.sentiment_label} /><ModePill mode={p.data_mode} />
        {p.entity_ids.map((e) => <span key={e} className="rounded bg-brand-50 px-1.5 text-brand-700">{names?.[e] ?? e}</span>)}
        {p.post_url && <ExtLink href={p.post_url}>original post</ExtLink>}
      </div>
    </li>
  );
}
