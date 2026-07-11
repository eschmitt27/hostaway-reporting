---
name: Chouette Patrimoine
colors:
  surface: '#f9f9ff'
  surface-dim: '#cfdaf6'
  surface-bright: '#f9f9ff'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f1f3ff'
  surface-container: '#e8edff'
  surface-container-high: '#e0e8ff'
  surface-container-highest: '#d7e2ff'
  on-surface: '#101b30'
  on-surface-variant: '#40484b'
  inverse-surface: '#263046'
  inverse-on-surface: '#edf0ff'
  outline: '#70787c'
  outline-variant: '#c0c8cb'
  surface-tint: '#306576'
  primary: '#003441'
  on-primary: '#ffffff'
  primary-container: '#0f4c5c'
  on-primary-container: '#87bbce'
  inverse-primary: '#9acee1'
  secondary: '#5b5f61'
  on-secondary: '#ffffff'
  secondary-container: '#e0e3e6'
  on-secondary-container: '#626567'
  tertiary: '#482700'
  on-tertiary: '#ffffff'
  tertiary-container: '#623d13'
  on-tertiary-container: '#dda975'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#b6ebfe'
  primary-fixed-dim: '#9acee1'
  on-primary-fixed: '#001f28'
  on-primary-fixed-variant: '#114d5d'
  secondary-fixed: '#e0e3e6'
  secondary-fixed-dim: '#c4c7ca'
  on-secondary-fixed: '#191c1e'
  on-secondary-fixed-variant: '#44474a'
  tertiary-fixed: '#ffdcbe'
  tertiary-fixed-dim: '#f3bc87'
  on-tertiary-fixed: '#2c1600'
  on-tertiary-fixed-variant: '#643e14'
  background: '#f9f9ff'
  on-background: '#101b30'
  surface-variant: '#d7e2ff'
typography:
  display-lg:
    fontFamily: Plus Jakarta Sans
    fontSize: 36px
    fontWeight: '700'
    lineHeight: '1.2'
    letterSpacing: -0.02em
  headline-md:
    fontFamily: Plus Jakarta Sans
    fontSize: 24px
    fontWeight: '600'
    lineHeight: '1.3'
  body-base:
    fontFamily: Plus Jakarta Sans
    fontSize: 16px
    fontWeight: '400'
    lineHeight: '1.6'
  body-sm:
    fontFamily: Plus Jakarta Sans
    fontSize: 14px
    fontWeight: '400'
    lineHeight: '1.5'
  tabular-data:
    fontFamily: Plus Jakarta Sans
    fontSize: 14px
    fontWeight: '500'
    lineHeight: '1.4'
  label-xs:
    fontFamily: Plus Jakarta Sans
    fontSize: 12px
    fontWeight: '600'
    lineHeight: '1'
rounded:
  sm: 0.25rem
  DEFAULT: 0.5rem
  md: 0.75rem
  lg: 1rem
  xl: 1.5rem
  full: 9999px
spacing:
  sidebar_width: 260px
  gutter: 24px
  container_padding: 32px
  stack_sm: 8px
  stack_md: 16px
  stack_lg: 32px
---

## Brand & Style

The brand identity for this design system centers on trust, meticulousness, and high-end asset management. It is designed for property owners and managers who require a sophisticated, sober interface that balances the "wisdom" of the owl (chouette) with the architectural stability of heritage (patrimoine).

The visual direction follows a **Corporate Modern** style with a focus on **High Information Hierarchy**. It prioritizes clarity and precision, utilizing clean lines and a structured layout to manage complex data points. The aesthetic is professional and established, avoiding trendy gimmicks in favor of a timeless, premium feel that conveys security and institutional reliability.

## Colors

The palette is anchored by a deep **Petrol Blue/Teal**, serving as the primary brand signifier to evoke stability and high-end professionalism. 

- **Primary**: A deep, saturated teal used for core brand elements, navigation highlights, and primary actions.
- **Surface**: A clean, cool white and light grey system to maintain a "sober" atmosphere.
- **Success (Green)**: Used for completed payments, confirmed reservations, and validated controls.
- **Warning (Orange)**: Reserved for pending tasks, upcoming deadlines, or maintenance alerts.
- **Error (Red)**: Specifically for blocking errors, unpaid balances, or critical system failures.
- **Neutral**: Slate-derived tones used for typography and secondary UI borders.

## Typography

This design system uses **Plus Jakarta Sans** exclusively. Its soft yet geometric structure provides a welcoming but highly modern feel. 

For property management and financial reporting, **tabular numbers** are a strict requirement to ensure data alignment in tables and lists. Use a slightly heavier weight (500) for numeric data to increase legibility. Headlines should utilize tighter letter-spacing for a refined, premium editorial look.

## Layout & Spacing

The layout is built around a **Fixed Left Sidebar** and a fluid content area that prioritizes density without sacrificing clarity.

- **Navigation**: The sidebar is permanent, containing the 10 core modules: *Accueil, Calendrier, Logements, Propriétaires & règlements, Réservations, Fournisseurs, Banques & caisse, Ménages, Sources & calculs, and Contrôles & clôture*.
- **Grid**: A 12-column grid is used for the main dashboard, with a focus on data-heavy tables.
- **Rhythm**: Consistent 8px increments. Tables use a compact vertical padding (12px) to maximize the amount of information visible on-screen at once.
- **Breakpoints**: 
  - **Desktop (1440px+)**: Full sidebar, multi-column dashboard.
  - **Tablet (768px-1024px)**: Sidebar collapses to icons; content switches to single/dual column.
  - **Mobile**: Sidebar becomes an off-canvas drawer.

## Elevation & Depth

To maintain a high-end, professional look, the design system uses **Tonal Layering** and **Low-Contrast Outlines** instead of heavy shadows.

- **Basemap**: The page background uses a very subtle off-white (`#F8F9FA`).
- **Cards/Containers**: White backgrounds with a fine 1px border in a light grey-blue.
- **Elevation**: Only the "Active" state of modals or selected menu items should use a soft, diffused ambient shadow to suggest a slight lift.
- **Separators**: Use subtle horizontal lines rather than physical depth to divide data sections, maintaining a "flat but layered" architectural feel.

## Shapes

The shape language reflects the stability of real estate. We use **Rounded** (8px) corners for standard UI components like cards and input fields. This provides a modern touch that softens the "coldness" of a corporate data tool. 

- **Buttons & Chips**: Use the 8px (standard) radius. Avoid pill shapes to maintain a serious, professional profile.
- **Sidebar Highlighters**: A subtle 4px radius on the active menu state background to keep the navigation feeling sharp and focused.

## Components

- **Buttons**: Primary buttons are solid Deep Teal with white text. Secondary buttons use a teal outline. No "ghost" buttons for primary actions.
- **Sidebar Menu**: Icons should be clean line-art. The active state is indicated by a background tint and a primary teal vertical bar on the left edge.
- **Data Tables**: The core of the system. Headers must be sticky, uppercase, and use the Label-XS style. Alternate row striping is used for readability.
- **Status Chips**: Small, high-contrast badges with background tints (e.g., Light Green background with Dark Green text for "Success").
- **Input Fields**: Labeled clearly above the field. Use a 1px border that thickens and changes to Primary Teal on focus.
- **Calendars**: High-density view with clear color-coding for different housing types or status levels.