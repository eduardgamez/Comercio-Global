from __future__ import annotations

import glob
import html
import os
import re
from datetime import datetime
from typing import Dict, List, Sequence, Tuple

import pandas as pd  # type: ignore
import plotly.express as px  # type: ignore
import plotly.graph_objects as go  # type: ignore
import plotly.io as pio  # type: ignore
import pyarrow.dataset as ds  # type: ignore


DATA_DIR = "data"
CACHE_FILE = os.path.join(DATA_DIR, "cache_comercio.parquet")
COUNTRY_CODES_FILE = os.path.join(DATA_DIR, "country_codes_V202601.csv")
PRODUCT_CODES_FILE = os.path.join(DATA_DIR, "product_codes_HS92_V202601.csv")
OUTPUT_FILE = "vista_comercio.html"


def format_value(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def combine_grouped(parts: List[pd.DataFrame], keys: Sequence[str]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame(columns=[*keys, "v"])
    merged = pd.concat(parts, ignore_index=True)
    return merged.groupby(list(keys), as_index=False, sort=False)["v"].sum()


def load_or_build_cache(data_dir: str = DATA_DIR, cache_file: str = CACHE_FILE) -> str:
    if os.path.exists(cache_file):
        print(f"Cargando cache Parquet: {cache_file}")
        return cache_file

    pattern = os.path.join(data_dir, "BACI_HS92_Y*_V202601.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError("No se encontraron CSV BACI en la carpeta data/.")

    print("No existe cache. Construyendo Parquet desde CSV (puede tardar)...")
    all_frames: List[pd.DataFrame] = []
    for file_path in files:
        file_name = os.path.basename(file_path)
        year_match = re.search(r"_Y(\d{4})", file_name)
        year_label = year_match.group(1) if year_match else "desconocido"
        print(f"  -> Cargando {file_name} ({year_label})")
        frame = pd.read_csv(
            file_path,
            usecols=["t", "i", "j", "k", "v", "q"],
            dtype={
                "t": "int16",
                "i": "int16",
                "j": "int16",
                "k": "int32",
                "v": "float32",
                "q": "float32",
            },
        )
        all_frames.append(frame)

    full_df = pd.concat(all_frames, ignore_index=True)
    full_df.to_parquet(cache_file, compression="snappy")
    print(f"Cache construido: {cache_file}")
    return cache_file


def load_lookup_tables() -> Tuple[Dict[int, str], Dict[int, str]]:
    country_df = pd.read_csv(COUNTRY_CODES_FILE)
    country_name_map = dict(
        zip(country_df["country_code"].astype(int), country_df["country_name"].astype(str))
    )

    product_df = pd.read_csv(PRODUCT_CODES_FILE, dtype={"code": "string", "description": "string"})
    product_df["k"] = pd.to_numeric(product_df["code"], errors="coerce").astype("Int64")
    product_df = product_df.dropna(subset=["k"])
    product_name_map = dict(
        zip(product_df["k"].astype(int), product_df["description"].astype(str))
    )

    return country_name_map, product_name_map


def aggregate_all_years(cache_file: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dataset = ds.dataset(cache_file, format="parquet")

    yearly_parts: List[pd.DataFrame] = []
    exporter_parts: List[pd.DataFrame] = []
    importer_parts: List[pd.DataFrame] = []
    product_parts: List[pd.DataFrame] = []

    print("Agregando metricas globales por lotes...")
    batch_count = 0
    for batch in dataset.to_batches(columns=["t", "i", "j", "k", "v"], batch_size=2_000_000):
        batch_count += 1
        frame = batch.to_pandas()
        frame["v"] = frame["v"].astype("float64")

        yearly_parts.append(frame.groupby(["t"], as_index=False, sort=False)["v"].sum())
        exporter_parts.append(frame.groupby(["t", "i"], as_index=False, sort=False)["v"].sum())
        importer_parts.append(frame.groupby(["t", "j"], as_index=False, sort=False)["v"].sum())
        product_parts.append(frame.groupby(["t", "k"], as_index=False, sort=False)["v"].sum())

        if batch_count % 10 == 0:
            print(f"  -> lotes procesados: {batch_count}")

    yearly = combine_grouped(yearly_parts, ["t"]).sort_values("t").reset_index(drop=True)
    exporter_year = combine_grouped(exporter_parts, ["t", "i"]).sort_values(["t", "v"], ascending=[True, False])
    importer_year = combine_grouped(importer_parts, ["t", "j"]).sort_values(["t", "v"], ascending=[True, False])
    product_year = combine_grouped(product_parts, ["t", "k"]).sort_values(["t", "v"], ascending=[True, False])

    return yearly, exporter_year, importer_year, product_year


def aggregate_latest_year(
    cache_file: str, latest_year: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dataset = ds.dataset(cache_file, format="parquet")
    filter_expr = ds.field("t") == latest_year

    exporter_parts: List[pd.DataFrame] = []
    importer_parts: List[pd.DataFrame] = []
    product_parts: List[pd.DataFrame] = []
    pair_parts: List[pd.DataFrame] = []
    exporter_product_parts: List[pd.DataFrame] = []

    print(f"Agregando detalle del ultimo anio ({latest_year})...")
    batch_count = 0
    for batch in dataset.to_batches(
        columns=["i", "j", "k", "v"],
        filter=filter_expr,
        batch_size=2_000_000,
    ):
        batch_count += 1
        frame = batch.to_pandas()
        frame["v"] = frame["v"].astype("float64")

        exporter_parts.append(frame.groupby(["i"], as_index=False, sort=False)["v"].sum())
        importer_parts.append(frame.groupby(["j"], as_index=False, sort=False)["v"].sum())
        product_parts.append(frame.groupby(["k"], as_index=False, sort=False)["v"].sum())
        pair_parts.append(frame.groupby(["i", "j"], as_index=False, sort=False)["v"].sum())
        exporter_product_parts.append(frame.groupby(["i", "k"], as_index=False, sort=False)["v"].sum())

        if batch_count % 10 == 0:
            print(f"  -> lotes ultimo anio procesados: {batch_count}")

    exporter_latest = combine_grouped(exporter_parts, ["i"]).sort_values("v", ascending=False)
    importer_latest = combine_grouped(importer_parts, ["j"]).sort_values("v", ascending=False)
    product_latest = combine_grouped(product_parts, ["k"]).sort_values("v", ascending=False)
    pair_latest = combine_grouped(pair_parts, ["i", "j"]).sort_values("v", ascending=False)
    exporter_product_latest = combine_grouped(exporter_product_parts, ["i", "k"]).sort_values(
        "v", ascending=False
    )

    return exporter_latest, importer_latest, product_latest, pair_latest, exporter_product_latest


def country_label(code: int, country_map: Dict[int, str]) -> str:
    return country_map.get(int(code), f"Pais {int(code)}")


def product_label(code: int, product_map: Dict[int, str]) -> str:
    return product_map.get(int(code), f"Producto {int(code):06d}")


def short_text(text: str, max_len: int = 74) -> str:
    clean = " ".join(str(text).split())
    if len(clean) <= max_len:
        return clean
    return clean[: max_len - 3] + "..."


def build_dashboard_html(
    figures: List[Tuple[str, str, go.Figure]],
    insights: List[str],
    kpis: List[Tuple[str, str]],
) -> str:
    figure_blocks: List[str] = []
    for idx, (title, subtitle, fig) in enumerate(figures):
        block = pio.to_html(
            fig,
            full_html=False,
            include_plotlyjs="inline" if idx == 0 else False,
            config={"responsive": True, "displayModeBar": False},
        )
        figure_blocks.append(
            f"""
            <section class="chart-card reveal">
              <h3>{html.escape(title)}</h3>
              <p>{html.escape(subtitle)}</p>
              {block}
            </section>
            """
        )

    insight_blocks = "".join(
        f'<article class="insight-card reveal"><p>{html.escape(item)}</p></article>' for item in insights
    )
    kpi_blocks = "".join(
        f"""
        <article class="kpi-card reveal">
          <span>{html.escape(label)}</span>
          <strong>{html.escape(value)}</strong>
        </article>
        """
        for label, value in kpis
    )

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Comercio Global - Inteligencia de Datos</title>
  <style>
    :root {{
      --bg-1: #e9f4f1;
      --bg-2: #f7efe2;
      --ink-1: #0f172a;
      --ink-2: #334155;
      --accent: #0b6e4f;
      --accent-2: #d9480f;
      --card: rgba(255, 255, 255, 0.82);
      --line: rgba(15, 23, 42, 0.1);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      color: var(--ink-1);
      font-family: "Avenir Next", "Segoe UI", "Trebuchet MS", sans-serif;
      background:
        radial-gradient(1200px 600px at 8% -15%, rgba(11, 110, 79, 0.24), transparent 60%),
        radial-gradient(1000px 580px at 100% 8%, rgba(217, 72, 15, 0.20), transparent 58%),
        linear-gradient(165deg, var(--bg-1), var(--bg-2));
      min-height: 100vh;
    }}

    .wrap {{
      width: min(1500px, 94vw);
      margin: 0 auto;
      padding: 2.3rem 0 3rem;
    }}

    .hero {{
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 1.5rem 1.6rem;
      background: var(--card);
      backdrop-filter: blur(6px);
      box-shadow: 0 22px 45px rgba(15, 23, 42, 0.10);
    }}

    .hero h1 {{
      margin: 0 0 0.7rem;
      font-size: clamp(1.8rem, 3.3vw, 3.3rem);
      line-height: 1.03;
      letter-spacing: -0.03em;
    }}

    .hero p {{
      margin: 0;
      color: var(--ink-2);
      font-size: 1rem;
      max-width: 68ch;
    }}

    .kpi-grid {{
      display: grid;
      gap: 0.85rem;
      margin-top: 1.1rem;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    }}

    .kpi-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: #fff;
      padding: 0.9rem 1rem;
      min-height: 102px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}

    .kpi-card span {{
      color: var(--ink-2);
      font-size: 0.84rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 600;
    }}

    .kpi-card strong {{
      font-size: clamp(1.2rem, 2vw, 1.75rem);
      letter-spacing: -0.02em;
    }}

    .section-title {{
      margin: 1.3rem 0 0.7rem;
      font-size: 1.18rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--ink-2);
    }}

    .insight-grid {{
      display: grid;
      gap: 0.9rem;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    }}

    .insight-card {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
      padding: 0.95rem 1rem;
      box-shadow: 0 9px 24px rgba(15, 23, 42, 0.05);
    }}

    .insight-card p {{
      margin: 0;
      color: var(--ink-1);
      line-height: 1.42;
      font-size: 0.97rem;
    }}

    .chart-grid {{
      margin-top: 0.8rem;
      display: grid;
      gap: 1rem;
      grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
    }}

    .chart-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fff;
      padding: 1rem 1rem 0.3rem;
      box-shadow: 0 15px 34px rgba(15, 23, 42, 0.07);
      overflow: hidden;
    }}

    .chart-card h3 {{
      margin: 0;
      font-size: 1.03rem;
      letter-spacing: -0.01em;
    }}

    .chart-card p {{
      margin: 0.32rem 0 0.65rem;
      color: var(--ink-2);
      font-size: 0.91rem;
    }}

    footer {{
      margin-top: 1.1rem;
      color: var(--ink-2);
      font-size: 0.8rem;
      text-align: right;
    }}

    .reveal {{
      opacity: 0;
      transform: translateY(16px);
      animation: reveal 0.75s ease-out forwards;
    }}

    .reveal:nth-child(2n) {{
      animation-delay: 0.08s;
    }}

    .reveal:nth-child(3n) {{
      animation-delay: 0.14s;
    }}

    @keyframes reveal {{
      to {{
        opacity: 1;
        transform: translateY(0);
      }}
    }}

    @media (max-width: 760px) {{
      .chart-grid {{
        grid-template-columns: 1fr;
      }}
      .wrap {{
        width: min(95vw, 95vw);
      }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero reveal">
      <h1>Radar de Inteligencia Comercial Global</h1>
      <p>
        Analisis sobre el dataset BACI 1995-2024 para detectar dinamicas profundas:
        choques sistemicos, hubs ocultos, concentracion productiva y cambios
        estructurales en los lideres del comercio mundial.
      </p>
      <div class="kpi-grid">
        {kpi_blocks}
      </div>
    </section>

    <h2 class="section-title">Lo Mas Interesante</h2>
    <section class="insight-grid">
      {insight_blocks}
    </section>

    <h2 class="section-title">Exploracion Visual</h2>
    <section class="chart-grid">
      {"".join(figure_blocks)}
    </section>

    <footer>Generado automaticamente el {generated_at}</footer>
  </main>
</body>
</html>
"""


def main() -> None:
    cache_file = load_or_build_cache()
    country_map, product_map = load_lookup_tables()

    yearly, exporter_year, importer_year, product_year = aggregate_all_years(cache_file)

    if yearly.empty:
        raise RuntimeError("No se pudo calcular la serie temporal global.")

    first_year = int(yearly["t"].min())
    latest_year = int(yearly["t"].max())
    year_span = max(latest_year - first_year, 1)

    (
        exporter_latest,
        importer_latest,
        product_latest,
        pair_latest,
        exporter_product_latest,
    ) = aggregate_latest_year(cache_file, latest_year)

    exporter_latest = exporter_latest.sort_values("v", ascending=False).reset_index(drop=True)
    importer_latest = importer_latest.sort_values("v", ascending=False).reset_index(drop=True)
    product_latest = product_latest.sort_values("v", ascending=False).reset_index(drop=True)
    pair_latest = pair_latest.sort_values("v", ascending=False).reset_index(drop=True)

    yearly = yearly.sort_values("t").reset_index(drop=True)
    yearly["yoy_pct"] = yearly["v"].pct_change() * 100.0

    latest_total = float(yearly.loc[yearly["t"] == latest_year, "v"].iloc[0])
    first_total = float(yearly.loc[yearly["t"] == first_year, "v"].iloc[0])
    total_growth_pct = ((latest_total / first_total) - 1.0) * 100.0 if first_total > 0 else 0.0
    cagr_total = ((latest_total / first_total) ** (1 / year_span) - 1.0) * 100.0 if first_total > 0 else 0.0

    yoy_valid = yearly.dropna(subset=["yoy_pct"])
    worst_row = yoy_valid.nsmallest(1, "yoy_pct").iloc[0] if not yoy_valid.empty else None
    best_row = yoy_valid.nlargest(1, "yoy_pct").iloc[0] if not yoy_valid.empty else None

    exporter_latest_top15 = exporter_latest.head(15).copy()
    importer_latest_top15 = importer_latest.head(15).copy()
    product_latest_top12 = product_latest.head(12).copy()

    exporter_latest_top15["pais"] = exporter_latest_top15["i"].map(
        lambda c: country_label(int(c), country_map)
    )
    importer_latest_top15["pais"] = importer_latest_top15["j"].map(
        lambda c: country_label(int(c), country_map)
    )
    product_latest_top12["producto"] = product_latest_top12["k"].map(
        lambda k: short_text(product_label(int(k), product_map), 62)
    )

    top_exporter_codes = exporter_latest_top15["i"].head(5).tolist()
    share_df = exporter_year[exporter_year["i"].isin(top_exporter_codes)].copy()
    share_df = share_df.merge(
        yearly[["t", "v"]].rename(columns={"v": "global_v"}),
        on="t",
        how="left",
    )
    share_df["share_pct"] = (share_df["v"] / share_df["global_v"]) * 100.0
    share_df["pais"] = share_df["i"].map(lambda c: country_label(int(c), country_map))

    country_growth = (
        exporter_year[exporter_year["t"].isin([first_year, latest_year])]
        .pivot_table(index="i", columns="t", values="v", aggfunc="sum", fill_value=0)
        .reset_index()
    )
    country_growth["start"] = country_growth.get(first_year, 0.0)
    country_growth["end"] = country_growth.get(latest_year, 0.0)
    threshold = float(country_growth["start"].quantile(0.60)) if not country_growth.empty else 0.0
    eligible_growth = country_growth[country_growth["start"] > max(threshold, 1.0)].copy()
    if eligible_growth.empty:
        eligible_growth = country_growth[country_growth["start"] > 1.0].copy()
    eligible_growth = eligible_growth[eligible_growth["end"] > 0].copy()
    if not eligible_growth.empty:
        eligible_growth["cagr_pct"] = (
            (eligible_growth["end"] / eligible_growth["start"]) ** (1 / year_span) - 1.0
        ) * 100.0
    else:
        eligible_growth["cagr_pct"] = []
    fastest_growth = eligible_growth.nlargest(10, "cagr_pct").copy()
    fastest_growth["pais"] = fastest_growth["i"].map(lambda c: country_label(int(c), country_map))

    momentum_base_year = 2010 if (yearly["t"] == 2010).any() else first_year
    product_growth = (
        product_year[product_year["t"].isin([momentum_base_year, latest_year])]
        .pivot_table(index="k", columns="t", values="v", aggfunc="sum", fill_value=0)
        .reset_index()
    )
    product_growth["start"] = product_growth.get(momentum_base_year, 0.0)
    product_growth["end"] = product_growth.get(latest_year, 0.0)
    product_threshold = float(product_growth["start"].quantile(0.75)) if not product_growth.empty else 0.0
    product_growth = product_growth[product_growth["start"] > max(product_threshold, 1.0)].copy()
    if not product_growth.empty:
        years_momentum = max(latest_year - momentum_base_year, 1)
        product_growth["cagr_pct"] = (
            (product_growth["end"] / product_growth["start"]) ** (1 / years_momentum) - 1.0
        ) * 100.0
        product_growth = product_growth.replace([float("inf"), -float("inf")], pd.NA).dropna(
            subset=["cagr_pct"]
        )
    else:
        product_growth["cagr_pct"] = []
    product_momentum_top = product_growth.nlargest(12, "cagr_pct").copy()
    product_momentum_top["producto"] = product_momentum_top["k"].map(
        lambda k: short_text(product_label(int(k), product_map), 62)
    )

    concentration_df = exporter_product_latest.copy()
    concentration_df["v2"] = concentration_df["v"] * concentration_df["v"]
    concentration_sum = concentration_df.groupby("i", as_index=False)["v2"].sum()
    exporter_totals = concentration_df.groupby("i", as_index=False)["v"].sum().rename(
        columns={"v": "total_v"}
    )
    concentration = exporter_totals.merge(concentration_sum, on="i", how="inner")
    concentration["hhi"] = concentration["v2"] / (concentration["total_v"] ** 2)
    concentration["effective_products"] = 1.0 / concentration["hhi"]

    top20_codes = exporter_latest.head(20)["i"].tolist()
    concentration_top = concentration[concentration["i"].isin(top20_codes)].copy()
    concentration_top["pais"] = concentration_top["i"].map(lambda c: country_label(int(c), country_map))
    most_diversified = concentration_top.nsmallest(1, "hhi")
    most_concentrated = concentration_top.nlargest(1, "hhi")

    partner_counts = pair_latest.groupby("i", as_index=False).size().rename(columns={"size": "partners"})
    hub_df = exporter_latest.merge(partner_counts, on="i", how="left").fillna({"partners": 0})
    hub_df["partners"] = hub_df["partners"].astype(int)
    hub_df["value_rank"] = hub_df["v"].rank(ascending=False, method="min")
    hidden_hubs = hub_df[hub_df["value_rank"] > 10].sort_values(
        ["partners", "v"], ascending=[False, False]
    )
    if hidden_hubs.empty:
        hidden_hub = hub_df.sort_values("partners", ascending=False).head(1)
    else:
        hidden_hub = hidden_hubs.head(1)

    top_corridors = pair_latest.head(20).copy()
    top_corridors["origen"] = top_corridors["i"].map(lambda c: country_label(int(c), country_map))
    top_corridors["destino"] = top_corridors["j"].map(lambda c: country_label(int(c), country_map))

    top_corridors_for_plot = top_corridors.copy()
    sankey_nodes = pd.Index(
        top_corridors_for_plot["origen"].tolist() + top_corridors_for_plot["destino"].tolist()
    ).unique()
    node_map = {name: idx for idx, name in enumerate(sankey_nodes)}
    top_corridors_for_plot["source_id"] = top_corridors_for_plot["origen"].map(node_map)
    top_corridors_for_plot["target_id"] = top_corridors_for_plot["destino"].map(node_map)

    fig_total = px.area(
        yearly,
        x="t",
        y="v",
        template="plotly_white",
        markers=True,
        labels={"t": "Anio", "v": "Valor total (campo v)"},
    )
    fig_total.update_traces(line_color="#0B6E4F", fillcolor="rgba(11, 110, 79, 0.25)")
    fig_total.update_layout(margin=dict(l=10, r=10, t=10, b=20))

    fig_yoy = px.bar(
        yoy_valid,
        x="t",
        y="yoy_pct",
        color=yoy_valid["yoy_pct"] >= 0,
        color_discrete_map={True: "#0B6E4F", False: "#D9480F"},
        template="plotly_white",
        labels={"t": "Anio", "yoy_pct": "Variacion interanual (%)", "color": "Crecimiento"},
    )
    fig_yoy.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=20))

    fig_exporters = px.bar(
        exporter_latest_top15.sort_values("v"),
        x="v",
        y="pais",
        orientation="h",
        template="plotly_white",
        color="v",
        color_continuous_scale=["#dceee8", "#0B6E4F"],
        labels={"v": "Valor exportado (campo v)", "pais": "Pais"},
    )
    fig_exporters.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=10, t=10, b=20))

    fig_share = px.line(
        share_df.sort_values("t"),
        x="t",
        y="share_pct",
        color="pais",
        markers=True,
        template="plotly_white",
        labels={"t": "Anio", "share_pct": "Cuota sobre comercio global (%)", "pais": "Pais"},
    )
    fig_share.update_layout(legend_title_text="", margin=dict(l=10, r=10, t=10, b=20))

    fig_momentum = px.bar(
        product_momentum_top.sort_values("cagr_pct"),
        x="cagr_pct",
        y="producto",
        orientation="h",
        template="plotly_white",
        color="cagr_pct",
        color_continuous_scale=["#fde2cc", "#d9480f"],
        labels={"cagr_pct": f"CAGR % ({momentum_base_year}-{latest_year})", "producto": "Producto"},
    )
    fig_momentum.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=10, t=10, b=20))

    fig_sankey = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node=dict(
                    label=sankey_nodes.tolist(),
                    color="rgba(15, 23, 42, 0.72)",
                    pad=14,
                    thickness=14,
                ),
                link=dict(
                    source=top_corridors_for_plot["source_id"],
                    target=top_corridors_for_plot["target_id"],
                    value=top_corridors_for_plot["v"],
                    color="rgba(11, 110, 79, 0.32)",
                ),
            )
        ]
    )
    fig_sankey.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=20),
        font=dict(size=11),
    )

    concentration_scatter = concentration.merge(
        exporter_latest[["i", "v"]].rename(columns={"v": "export_value"}),
        on="i",
        how="inner",
    )
    concentration_scatter["pais"] = concentration_scatter["i"].map(
        lambda c: country_label(int(c), country_map)
    )
    concentration_scatter = concentration_scatter.sort_values("export_value", ascending=False).head(50)
    fig_concentration = px.scatter(
        concentration_scatter,
        x="hhi",
        y="export_value",
        size="effective_products",
        hover_name="pais",
        template="plotly_white",
        labels={
            "hhi": "Concentracion exportadora (HHI, menor = mas diversificado)",
            "export_value": "Valor exportado ultimo anio",
            "effective_products": "Productos efectivos",
        },
        color="effective_products",
        color_continuous_scale=["#fff3df", "#0b6e4f"],
    )
    fig_concentration.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=10, t=10, b=20))

    fastest_row = fastest_growth.iloc[0] if not fastest_growth.empty else None
    diversified_row = most_diversified.iloc[0] if not most_diversified.empty else None
    concentrated_row = most_concentrated.iloc[0] if not most_concentrated.empty else None
    hidden_hub_row = hidden_hub.iloc[0] if not hidden_hub.empty else None

    insights: List[str] = []
    insights.append(
        f"Entre {first_year} y {latest_year}, el comercio global crecio un {total_growth_pct:.1f}% "
        f"(CAGR {cagr_total:.2f}% anual)."
    )

    if worst_row is not None and best_row is not None:
        insights.append(
            f"El mayor shock fue {int(worst_row['t'])} ({worst_row['yoy_pct']:.2f}% interanual) "
            f"y el mayor rebote fue {int(best_row['t'])} ({best_row['yoy_pct']:.2f}%)."
        )

    if fastest_row is not None:
        insights.append(
            f"Crecimiento estructural destacado: {country_label(int(fastest_row['i']), country_map)} "
            f"lidera entre los paises grandes con CAGR exportador de {fastest_row['cagr_pct']:.2f}%."
        )

    if hidden_hub_row is not None:
        insights.append(
            f"Hub oculto detectado: {country_label(int(hidden_hub_row['i']), country_map)} tiene "
            f"{int(hidden_hub_row['partners'])} socios activos en {latest_year}, con posicion "
            f"comercial fuera del top 10 por valor."
        )

    if diversified_row is not None and concentrated_row is not None:
        insights.append(
            f"Diversificacion vs especializacion ({latest_year}): {country_label(int(diversified_row['i']), country_map)} "
            f"es el mas diversificado dentro del top exportador, mientras "
            f"{country_label(int(concentrated_row['i']), country_map)} concentra mas su canasta."
        )

    top_corridor_row = top_corridors.iloc[0] if not top_corridors.empty else None
    if top_corridor_row is not None:
        insights.append(
            f"Corredor bilateral dominante en {latest_year}: "
            f"{top_corridor_row['origen']} -> {top_corridor_row['destino']} "
            f"con valor {format_value(float(top_corridor_row['v']))}."
        )

    active_countries = len(set(exporter_year["i"].unique()).union(set(importer_year["j"].unique())))
    active_products = int(product_year["k"].nunique())

    kpis = [
        ("Periodo analizado", f"{first_year}-{latest_year}"),
        ("Comercio total acumulado", format_value(float(yearly["v"].sum()))),
        ("Ultimo anio (valor)", format_value(latest_total)),
        ("Paises activos", str(active_countries)),
        ("Productos activos", str(active_products)),
        ("Corredores activos (ultimo anio)", str(int(pair_latest.shape[0]))),
    ]

    figures: List[Tuple[str, str, go.Figure]] = [
        (
            "Pulso del Comercio Global",
            "Serie anual agregada sobre el campo v del dataset BACI.",
            fig_total,
        ),
        (
            "Choques y Rebotes Interanuales",
            "Variacion porcentual anio contra anio para detectar rupturas de tendencia.",
            fig_yoy,
        ),
        (
            f"Top Exportadores ({latest_year})",
            "Ranking por valor exportado del ultimo anio disponible.",
            fig_exporters,
        ),
        (
            "Evolucion de Cuotas de Liderazgo",
            "Como cambia el peso relativo de los lideres exportadores a lo largo del tiempo.",
            fig_share,
        ),
        (
            "Productos con Mayor Aceleracion",
            f"Productos de gran base que mas crecen entre {momentum_base_year} y {latest_year}.",
            fig_momentum,
        ),
        (
            f"Top Corredores Bilaterales ({latest_year})",
            "Mapa de flujos de alto impacto entre origen y destino.",
            fig_sankey,
        ),
        (
            "Mapa de Diversificacion Exportadora",
            "Cuanto diversifica cada potencia comercial: concentracion (HHI) vs valor.",
            fig_concentration,
        ),
    ]

    dashboard_html = build_dashboard_html(figures, insights, kpis)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as output:
        output.write(dashboard_html)

    print(f"\nDashboard generado: {OUTPUT_FILE}")
    print("Resumen rapido:")
    for idx, line in enumerate(insights, start=1):
        print(f"  {idx}. {line}")


if __name__ == "__main__":
    main()
