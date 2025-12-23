// demo_swaths.js
// Hard-coded hail swaths for demo purposes.
// You can add as many as you like. The frontend code will render them all.

window.demoSwaths = [
  {
    id: "az_scottsdale_2025_11_23",
    label: "Scottsdale AZ – Nov 23, 2025",
    state: "AZ",
    date: "2025-11-23",
    maxHailInches: 1.75,
    color: "#ff0000",
    polygon: [
      // Rough swath over Scottsdale area (you can tweak later)
      { lat: 33.700, lng: -112.050 },
      { lat: 33.700, lng: -111.900 },
      { lat: 33.640, lng: -111.850 },
      { lat: 33.580, lng: -111.870 },
      { lat: 33.580, lng: -112.020 }
    ]
  },
  {
    id: "az_scottsdale_2024_04_10",
    label: "Scottsdale AZ – Apr 10, 2024",
    state: "AZ",
    date: "2024-04-10",
    maxHailInches: 2.25,
    color: "#ff7f00",
    polygon: [
      { lat: 33.640, lng: -112.100 },
      { lat: 33.640, lng: -111.930 },
      { lat: 33.600, lng: -111.880 },
      { lat: 33.540, lng: -111.900 },
      { lat: 33.540, lng: -112.070 }
    ]
  },
  {
    id: "mo_kc_2024_05_02",
    label: "Kansas City MO – May 2, 2024",
    state: "MO",
    date: "2024-05-02",
    maxHailInches: 2.75,
    color: "#ff00ff",
    polygon: [
      { lat: 39.200, lng: -94.700 },
      { lat: 39.200, lng: -94.450 },
      { lat: 39.050, lng: -94.450 },
      { lat: 39.050, lng: -94.700 }
    ]
  }
];
