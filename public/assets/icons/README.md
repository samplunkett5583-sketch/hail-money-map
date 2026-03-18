# Hail Money Map – Icon Set

This directory contains the complete icon set for the Hail Money Map PWA, optimized for Android, iOS, and web.

## Directory Structure

```
public/assets/icons/
├── svg/                        # Source SVG files (full 1024×1024 design)
│   ├── app_icon_vector_traced.svg
│   ├── estimates-icon_vector.svg
│   ├── all_company_jobs-icon_vector.svg
│   └── campaign-icon_vector.svg
├── icon-1024.svg               # App Store / Play Store
├── icon-512.svg                # PWA web manifest
├── icon-192.svg                # PWA / Android
├── icon-180.svg                # iOS @3x
├── icon-167.svg                # iPad @2x
├── icon-152.svg                # iPad @2x
├── icon-144.svg                # Android XXXHDPI
├── icon-120.svg                # iOS @2x
├── icon-96.svg                 # Android XHDPI
├── icon-72.svg                 # Android HDPI
├── icon-48.svg                 # Android MDPI
├── icon-32.svg                 # Favicon
├── icon-16.svg                 # Favicon small
├── estimates-icon-24.svg       # UI icon 24×24
├── estimates-icon-32.svg       # UI icon 32×32
├── estimates-icon-48.svg       # UI icon 48×48
├── all_company_jobs-icon-24.svg
├── all_company_jobs-icon-32.svg
├── all_company_jobs-icon-48.svg
├── campaign-icon-24.svg
├── campaign-icon-32.svg
├── campaign-icon-48.svg
└── README.md
```

## Color Palette

| Role | Color |
|------|-------|
| Primary dark blue | `#15345c` |
| Primary medium blue | `#145892` |
| Primary light blue | `#1b7ec4` |
| Accent green dark | `#589866` |
| Accent green mid | `#9ad562` |
| Accent green light | `#c0eb73` |
| Hail yellow | `#fdfe43` |
| Campaign red | `#f61321` |

## Platform Targets

| Size | Platform / Use |
|------|---------------|
| 1024×1024 | App Store (iOS) / Play Store (Android) |
| 512×512 | PWA web manifest (Chrome install prompt) |
| 192×192 | PWA / Android home screen |
| 180×180 | iOS @3x (iPhone Plus/Pro Max) |
| 167×167 | iPad Pro @2x |
| 152×152 | iPad @2x |
| 144×144 | Android XXXHDPI |
| 120×120 | iOS @2x |
| 96×96 | Android XHDPI |
| 72×72 | Android HDPI |
| 48×48 | Android MDPI / UI icon large |
| 32×32 | Browser favicon |
| 16×16 | Browser favicon small |

## Usage

### In HTML (head section)
```html
<link rel="apple-touch-icon" sizes="180x180" href="/assets/icons/icon-180.svg">
<link rel="apple-touch-icon" sizes="152x152" href="/assets/icons/icon-152.svg">
<link rel="apple-touch-icon" sizes="120x120" href="/assets/icons/icon-120.svg">
<link rel="icon" type="image/svg+xml" sizes="32x32" href="/assets/icons/icon-32.svg">
<link rel="icon" type="image/svg+xml" sizes="16x16" href="/assets/icons/icon-16.svg">
```

### In CSS (for UI icons)
```css
.estimates-icon { content: url('/assets/icons/estimates-icon-24.svg'); }
.jobs-icon      { content: url('/assets/icons/all_company_jobs-icon-24.svg'); }
.campaign-icon  { content: url('/assets/icons/campaign-icon-24.svg'); }
```

## Notes
- All SVGs use `viewBox="0 0 1024 1024"` for consistent scaling
- Width/height attributes are set per file to declare the intended display size
- Do NOT alter path data — only `width`, `height`, and `viewBox` should ever be changed
- Source files in `svg/` directory contain the full detail at 1024×1024
