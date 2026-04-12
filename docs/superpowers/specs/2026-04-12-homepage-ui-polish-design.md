# Homepage UI Polish Design

- Date: 2026-04-12
- Scope: Home page visual polish for `public/index.html` and `public/replica.css`
- Status: Approved design, pending user review before implementation planning

## Summary

Refine the home page with a `hybrid quiet hierarchy` direction that sits between a dashboard and a professional report. Keep the current information density, content footprint, primary sections, and interaction model, while improving:

- hierarchy readability across the overview, holdings, chart, and detail sections
- spacing rhythm inside and between sections
- control and state consistency for tabs, month controls, summary cards, and tables
- mobile composition by recomposing priorities instead of squeezing the desktop layout

The intended result is a page that scans quickly like a dashboard, but reads steadily like a report.

## Current Context

The page is currently defined by [public/index.html](/Users/kale/Documents/openclaw/stock-lu/public/index.html) and [public/replica.css](/Users/kale/Documents/openclaw/stock-lu/public/replica.css), with shared tokens in [public/design-tokens.css](/Users/kale/Documents/openclaw/stock-lu/public/design-tokens.css).

The current implementation already has a strong base:

- clear top-level sections for overview, holdings, chart, and monthly details
- compact metric cards and data-dense tables
- responsive breakpoints already in place
- existing color tokens and state tokens that support a more coherent system

The main weakness is not missing functionality. It is visual competition:

- top-level panels all present with similar surface strength
- section headers, tools, and summary elements compete for attention
- holdings controls and subsection cards feel more card-like than report-like
- interactive states are readable, but not unified enough to feel calm and deliberate

## Goals

1. Preserve information density and current content footprint.
2. Keep the page between dashboard and report, rather than pushing fully into either extreme.
3. Make the three main experience zones feel balanced:
   - top overview
   - holdings and adjustments
   - chart area
4. Use moderate section separation, not one continuous sheet and not a grid of loud standalone cards.
5. Make default states quiet and interaction states clear without becoming bright or theatrical.
6. Keep mobile readable by restructuring alignment and flow, not by shrinking everything.

## Non-Goals

- No new modules, banners, hero sections, or explanatory copy blocks
- No changes to data sources, chart logic, or table business logic
- No reduction of visible metrics or table columns on desktop
- No redesign of the product into a new dashboard shell or admin console
- No decorative motion system beyond short state transitions

## Design Thesis

Use `section rhythm` as the primary hierarchy tool and `surface restraint` as the secondary tool.

This means:

- fewer cues should do more work
- section boundaries stay visible, but softer
- internal spacing should explain grouping before borders or shadows do
- selected and active states should look intentional, not loud
- the page should still hold up in grayscale if gradients and shadows are reduced

## Planned Rewrite Depth

This is a `medium restructure` of the page presentation.

- Primary implementation surface: `public/replica.css`
- Allowed structural adjustments in `public/index.html`: small wrapper or grouping changes only when needed to support cleaner section heads, summary alignment, or tool placement
- No expected JavaScript behavior changes

## Page-Level Structure

### Overall Section Rhythm

The page should read as four related sections in one system:

1. Overview shell
2. Holdings shell
3. Chart shell
4. Detail list shell

Planned adjustments:

- normalize vertical spacing between these shells so the page feels paced, not stacked arbitrarily
- keep each shell distinct, but reduce the sense that every shell is floating independently
- bias hierarchy toward content order rather than surface intensity

### Surface Strategy

All four major shells should share one quieter family of surfaces, with the overview shell still slightly leading.

Planned adjustments:

- reduce border and shadow contrast on secondary shells
- keep the overview shell as the highest-emphasis region, but with cleaner internal grouping
- let chart and detail sections inherit the same section language with only lighter emphasis differences

## Section Designs

### 1. Overview Section

Intent: highest-recognition summary area without looking like five unrelated widgets.

Planned adjustments:

- keep the overview block as the first and strongest section
- tighten the `dashboard-shell-top` spacing so heading, month meta, and month selector feel like one header row
- maintain the five metrics, but shift them visually from `five separate cards` to `one metric group with five cells`
- reduce the apparent weight of metric boundaries while preserving scanability
- strengthen the first metric as the lead metric through type balance and rhythm, not through a louder accent

Metric card guidelines:

- consistent vertical spacing between label, value, and meta
- stable minimum heights to prevent uneven rows
- calmer separators between metrics
- no increase in whitespace that would reduce density

### 2. Holdings Section

Intent: the main working body of the page and the most report-like reading experience.

Planned adjustments:

- make the section head feel like a report header instead of a title block plus a separate floating control
- keep the title, meta, and view toggle in one readable line of attention
- reduce the pill/toggle prominence in the default state
- maintain strong selected state for the current view, but remove excessive lift or glow

Table container guidelines:

- soften the table shell so it feels anchored to the section, not like a card inside a card
- let header rows, group rows, and normal rows differentiate mainly through tint, weight, and spacing
- preserve horizontal scroll affordance on smaller screens
- keep the table readable in dense mode

Subsection card guidelines for `new opens` and `closed positions`:

- treat them as supporting modules inside the holdings section
- reduce redundant card chrome so they do not compete with the main table
- align their header and body spacing to the same rhythm as the main holdings section

### 3. Chart Section

Intent: secondary analysis area with a stable transition from summary cards into the canvas.

Planned adjustments:

- match the section-head rhythm used in holdings and details
- simplify the summary cards so they feel like annotations to the chart rather than standalone stat tiles
- unify the chart summary cards with the same control radius, border strength, and surface logic used elsewhere
- keep the chart canvas visually present, but avoid overselling it with strong framing

### 4. Detail List Section

Intent: tertiary section that still belongs to the same page system.

Planned adjustments:

- keep the title and meta structure aligned with chart and holdings
- preserve the current density of the detail list
- ensure the section transition into details feels intentional and not like a leftover module

## Spacing System

Spacing changes should be systematic, not case-by-case.

### Between Sections

- unify outer section gaps into one consistent rhythm
- make the top overview-to-holdings transition slightly more pronounced than later transitions, but within the same system

### Within Section Headers

Use one internal order:

1. eyebrow
2. title
3. meta
4. tools or toggle
5. content body

Planned adjustments:

- reduce inconsistent vertical gaps between eyebrow, title, and meta
- align tool groups to the same baseline logic across sections
- keep tool groups visually subordinate to titles

### Within Data Modules

- metric cards, summary cards, subsection cards, and table shells should share more compatible internal padding values
- small support text should sit closer to its primary value
- top and bottom paddings should feel deliberate rather than inherited from unrelated components

## Interaction State System

The interaction language should be `quiet by default, clear on intent`.

### Default State

- low-contrast surfaces
- restrained borders
- minimal shadow lift
- clear text contrast

### Hover State

- use slight border or background shifts only
- avoid strong elevation jumps
- avoid making passive elements look selected

### Active / Selected State

- use one consistent selected treatment for tab-like controls and chips
- selected state should be distinct from hover and pressed state
- use tint and contrast, not glow

### Focus-Visible State

- maintain a clear keyboard focus ring
- keep focus-visible independent from pointer hover styling
- ensure focus is still visible on quiet surfaces

### Motion

- short, consistent transitions only
- motion should reinforce interaction logic, not attract attention
- respect existing `prefers-reduced-motion` handling

## Responsive Strategy

Desktop and mobile should preserve the same priorities while changing composition.

### Desktop

- preserve current information density
- improve alignment and hierarchy without creating more whitespace-heavy cards

### Tablet

- keep section headers readable when they wrap
- avoid controls feeling detached from the content they govern

### Mobile

- recompose the order and alignment of title groups, toggles, and selectors
- keep the overview metrics compact but more uniform
- maintain table scrolling behavior without adding visual clutter
- avoid solving density by only shrinking fonts

## Accessibility and Readability

The polish pass should preserve or improve:

- keyboard focus visibility
- contrast of primary text and numeric values
- touch target clarity for tabs and month selectors
- scanability of numeric data and table headers

## Implementation Notes

Expected file touch order:

1. `public/replica.css`
2. `public/index.html` only if wrapper or header grouping changes are required
3. `public/design-tokens.css` only if existing tokens are insufficient to support the refined spacing or state system

Implementation guidance:

- prefer reusing existing tokens before introducing new ones
- if new tokens are necessary, keep them few and system-level
- follow the existing page structure unless a small HTML adjustment clearly reduces CSS complexity

## Acceptance Criteria

The redesign is successful if:

1. The page still contains the same major sections, metrics, tables, and chart content.
2. The overview, holdings, and chart areas feel balanced rather than competing.
3. The holdings section reads as the page body rather than as another equal-weight card block.
4. Default states feel calmer, while selected and focus-visible states stay clear.
5. The page feels moderately segmented, not over-carded and not flattened into a single sheet.
6. Mobile layouts feel intentionally recomposed rather than squeezed.
7. Visual improvements come mainly from hierarchy, spacing, and state consistency instead of extra decoration.

## Verification Plan

Before implementation is considered complete, verify:

- desktop layout at wide and mid-width breakpoints
- mobile layout around the existing 960px and 720px transitions
- overview metric grouping and lead-metric emphasis
- holdings title/toggle alignment in both desktop and mobile layouts
- table shell weight, header readability, and scroll affordance
- chart summary cards and canvas framing
- hover, active, and focus-visible states for month selector and view toggle
- no accidental information density loss

## Open Questions Resolved During Brainstorming

- Visual direction: between dashboard and professional report
- Area priority: balanced across overview, holdings, and chart
- Segmentation level: moderate section separation
- Interaction tone: quiet and clear without drawing attention
