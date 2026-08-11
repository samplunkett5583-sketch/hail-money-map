# Hail Money Launch Checklist

- [ ] Start the Regrid 30-day API trial immediately before launch, then connect the residential address/unit dataset for accurate housing-unit counts inside each storm swath.
- [ ] Move the development-only YouTube storm-picture search from the browser into the secured `searchStormMedia` Firebase function, enable billing, store `YOUTUBE_API_KEY` in Secret Manager, deploy the function, and remove the browser-stored API key.
- [ ] Finalize and restrict the production Google Maps API key for the Maps page.
- [ ] Finalize and restrict the production Google Maps API key used by Campaigns, including every approved production domain.
