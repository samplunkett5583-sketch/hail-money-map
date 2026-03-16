// demo_swaths.js
// Hard-coded hail swaths for demo purposes – elongated, radar-style shapes.
// Each swath has multiple intensity layers (outer→inner) like real NOAA MESH radar.
// layers[0] = outer (green, light), layers[N-1] = core (red, intense).

window.demoSwaths = [
  {
    id: "az_scottsdale_2025_11_23",
    label: "Scottsdale AZ – Nov 23, 2025",
    state: "AZ",
    date: "2025-11-23",
    maxHailInches: 1.75,
    // Elongated SW→NE storm track, organic multi-point outline
    layers: [
      {
        // Outer green band (light hail, ≥0.75 in)
        color: "#00cc44",
        opacity: 0.28,
        polygon: [
          { lat: 33.530, lng: -112.150 }, { lat: 33.560, lng: -112.080 },
          { lat: 33.590, lng: -111.990 }, { lat: 33.630, lng: -111.900 },
          { lat: 33.670, lng: -111.830 }, { lat: 33.710, lng: -111.790 },
          { lat: 33.740, lng: -111.810 }, { lat: 33.730, lng: -111.870 },
          { lat: 33.700, lng: -111.950 }, { lat: 33.660, lng: -112.030 },
          { lat: 33.620, lng: -112.090 }, { lat: 33.580, lng: -112.140 },
          { lat: 33.550, lng: -112.160 }
        ]
      },
      {
        // Middle yellow band (moderate hail, ≥1.25 in)
        color: "#ffdd00",
        opacity: 0.35,
        polygon: [
          { lat: 33.570, lng: -112.090 }, { lat: 33.600, lng: -112.020 },
          { lat: 33.630, lng: -111.950 }, { lat: 33.660, lng: -111.880 },
          { lat: 33.695, lng: -111.840 }, { lat: 33.720, lng: -111.845 },
          { lat: 33.715, lng: -111.895 }, { lat: 33.685, lng: -111.965 },
          { lat: 33.650, lng: -112.040 }, { lat: 33.610, lng: -112.080 },
          { lat: 33.585, lng: -112.095 }
        ]
      },
      {
        // Inner orange band (large hail, ≥1.75 in)
        color: "#ff7700",
        opacity: 0.45,
        polygon: [
          { lat: 33.600, lng: -112.040 }, { lat: 33.625, lng: -111.985 },
          { lat: 33.655, lng: -111.925 }, { lat: 33.685, lng: -111.880 },
          { lat: 33.710, lng: -111.862 }, { lat: 33.705, lng: -111.905 },
          { lat: 33.675, lng: -111.960 }, { lat: 33.645, lng: -112.015 },
          { lat: 33.615, lng: -112.042 }
        ]
      },
      {
        // Core red (severe hail core)
        color: "#ff2200",
        opacity: 0.55,
        polygon: [
          { lat: 33.628, lng: -112.010 }, { lat: 33.645, lng: -111.968 },
          { lat: 33.667, lng: -111.932 }, { lat: 33.688, lng: -111.900 },
          { lat: 33.685, lng: -111.925 }, { lat: 33.662, lng: -111.960 },
          { lat: 33.640, lng: -112.002 }
        ]
      }
    ]
  },

  {
    id: "az_scottsdale_2024_04_10",
    label: "Scottsdale AZ – Apr 10, 2024",
    state: "AZ",
    date: "2024-04-10",
    maxHailInches: 2.25,
    layers: [
      {
        color: "#00cc44",
        opacity: 0.28,
        polygon: [
          { lat: 33.480, lng: -112.200 }, { lat: 33.510, lng: -112.120 },
          { lat: 33.545, lng: -112.030 }, { lat: 33.585, lng: -111.940 },
          { lat: 33.625, lng: -111.860 }, { lat: 33.660, lng: -111.800 },
          { lat: 33.690, lng: -111.810 }, { lat: 33.680, lng: -111.870 },
          { lat: 33.645, lng: -111.950 }, { lat: 33.600, lng: -112.030 },
          { lat: 33.555, lng: -112.100 }, { lat: 33.510, lng: -112.170 },
          { lat: 33.490, lng: -112.205 }
        ]
      },
      {
        color: "#ffdd00",
        opacity: 0.36,
        polygon: [
          { lat: 33.510, lng: -112.150 }, { lat: 33.545, lng: -112.070 },
          { lat: 33.580, lng: -111.985 }, { lat: 33.618, lng: -111.905 },
          { lat: 33.652, lng: -111.840 }, { lat: 33.675, lng: -111.830 },
          { lat: 33.668, lng: -111.885 }, { lat: 33.630, lng: -111.965 },
          { lat: 33.590, lng: -112.045 }, { lat: 33.548, lng: -112.118 },
          { lat: 33.518, lng: -112.152 }
        ]
      },
      {
        color: "#ff7700",
        opacity: 0.46,
        polygon: [
          { lat: 33.540, lng: -112.105 }, { lat: 33.572, lng: -112.030 },
          { lat: 33.608, lng: -111.950 }, { lat: 33.645, lng: -111.874 },
          { lat: 33.665, lng: -111.850 }, { lat: 33.658, lng: -111.900 },
          { lat: 33.620, lng: -111.980 }, { lat: 33.582, lng: -112.058 },
          { lat: 33.550, lng: -112.108 }
        ]
      },
      {
        color: "#ff2200",
        opacity: 0.58,
        polygon: [
          { lat: 33.565, lng: -112.068 }, { lat: 33.590, lng: -112.008 },
          { lat: 33.622, lng: -111.936 }, { lat: 33.650, lng: -111.873 },
          { lat: 33.645, lng: -111.900 }, { lat: 33.614, lng: -111.968 },
          { lat: 33.582, lng: -112.038 }, { lat: 33.562, lng: -112.070 }
        ]
      }
    ]
  },

  {
    id: "mo_kc_2024_05_02",
    label: "Kansas City MO – May 2, 2024",
    state: "MO",
    date: "2024-05-02",
    maxHailInches: 2.75,
    layers: [
      {
        color: "#00cc44",
        opacity: 0.28,
        polygon: [
          { lat: 38.920, lng: -94.850 }, { lat: 38.960, lng: -94.760 },
          { lat: 39.010, lng: -94.650 }, { lat: 39.065, lng: -94.530 },
          { lat: 39.120, lng: -94.410 }, { lat: 39.175, lng: -94.300 },
          { lat: 39.215, lng: -94.260 }, { lat: 39.240, lng: -94.280 },
          { lat: 39.220, lng: -94.360 }, { lat: 39.165, lng: -94.480 },
          { lat: 39.105, lng: -94.600 }, { lat: 39.040, lng: -94.720 },
          { lat: 38.980, lng: -94.820 }, { lat: 38.940, lng: -94.860 }
        ]
      },
      {
        color: "#ffdd00",
        opacity: 0.36,
        polygon: [
          { lat: 38.950, lng: -94.800 }, { lat: 38.988, lng: -94.715 },
          { lat: 39.038, lng: -94.610 }, { lat: 39.092, lng: -94.490 },
          { lat: 39.148, lng: -94.365 }, { lat: 39.200, lng: -94.288 },
          { lat: 39.225, lng: -94.296 }, { lat: 39.202, lng: -94.380 },
          { lat: 39.142, lng: -94.505 }, { lat: 39.082, lng: -94.630 },
          { lat: 39.020, lng: -94.748 }, { lat: 38.963, lng: -94.812 }
        ]
      },
      {
        color: "#ff7700",
        opacity: 0.46,
        polygon: [
          { lat: 38.978, lng: -94.750 }, { lat: 39.015, lng: -94.665 },
          { lat: 39.062, lng: -94.558 }, { lat: 39.118, lng: -94.434 },
          { lat: 39.172, lng: -94.313 }, { lat: 39.210, lng: -94.278 },
          { lat: 39.205, lng: -94.340 }, { lat: 39.148, lng: -94.464 },
          { lat: 39.090, lng: -94.590 }, { lat: 39.038, lng: -94.692 },
          { lat: 38.992, lng: -94.762 }
        ]
      },
      {
        color: "#ff2200",
        opacity: 0.60,
        polygon: [
          { lat: 39.010, lng: -94.700 }, { lat: 39.048, lng: -94.615 },
          { lat: 39.092, lng: -94.510 }, { lat: 39.145, lng: -94.392 },
          { lat: 39.192, lng: -94.296 }, { lat: 39.188, lng: -94.355 },
          { lat: 39.135, lng: -94.472 }, { lat: 39.082, lng: -94.578 },
          { lat: 39.040, lng: -94.670 }
        ]
      }
    ]
  },

  {
    id: "tx_dfw_2024_06_15",
    label: "DFW TX – Jun 15, 2024",
    state: "TX",
    date: "2024-06-15",
    maxHailInches: 3.0,
    layers: [
      {
        color: "#00cc44",
        opacity: 0.28,
        polygon: [
          { lat: 32.600, lng: -97.350 }, { lat: 32.650, lng: -97.220 },
          { lat: 32.720, lng: -97.070 }, { lat: 32.800, lng: -96.910 },
          { lat: 32.880, lng: -96.760 }, { lat: 32.940, lng: -96.650 },
          { lat: 32.975, lng: -96.630 }, { lat: 32.960, lng: -96.700 },
          { lat: 32.895, lng: -96.840 }, { lat: 32.815, lng: -96.990 },
          { lat: 32.735, lng: -97.145 }, { lat: 32.655, lng: -97.295 },
          { lat: 32.615, lng: -97.360 }
        ]
      },
      {
        color: "#ffdd00",
        opacity: 0.37,
        polygon: [
          { lat: 32.630, lng: -97.295 }, { lat: 32.680, lng: -97.170 },
          { lat: 32.748, lng: -97.025 }, { lat: 32.826, lng: -96.868 },
          { lat: 32.906, lng: -96.718 }, { lat: 32.958, lng: -96.648 },
          { lat: 32.950, lng: -96.705 }, { lat: 32.892, lng: -96.855 },
          { lat: 32.812, lng: -97.008 }, { lat: 32.734, lng: -97.162 },
          { lat: 32.661, lng: -97.302 }
        ]
      },
      {
        color: "#ff7700",
        opacity: 0.48,
        polygon: [
          { lat: 32.662, lng: -97.240 }, { lat: 32.710, lng: -97.118 },
          { lat: 32.775, lng: -96.975 }, { lat: 32.851, lng: -96.822 },
          { lat: 32.928, lng: -96.672 }, { lat: 32.940, lng: -96.675 },
          { lat: 32.918, lng: -96.742 }, { lat: 32.838, lng: -96.895 },
          { lat: 32.762, lng: -97.042 }, { lat: 32.690, lng: -97.188 },
          { lat: 32.650, lng: -97.250 }
        ]
      },
      {
        color: "#ff2200",
        opacity: 0.62,
        polygon: [
          { lat: 32.695, lng: -97.185 }, { lat: 32.740, lng: -97.068 },
          { lat: 32.800, lng: -96.930 }, { lat: 32.870, lng: -96.792 },
          { lat: 32.930, lng: -96.678 }, { lat: 32.920, lng: -96.720 },
          { lat: 32.858, lng: -96.855 }, { lat: 32.790, lng: -96.992 },
          { lat: 32.728, lng: -97.130 }, { lat: 32.690, lng: -97.200 }
        ]
      }
    ]
  },

  {
    id: "co_denver_2024_07_28",
    label: "Denver CO – Jul 28, 2024",
    state: "CO",
    date: "2024-07-28",
    maxHailInches: 1.5,
    layers: [
      {
        color: "#00cc44",
        opacity: 0.27,
        polygon: [
          { lat: 39.580, lng: -105.050 }, { lat: 39.620, lng: -104.960 },
          { lat: 39.670, lng: -104.850 }, { lat: 39.720, lng: -104.730 },
          { lat: 39.760, lng: -104.630 }, { lat: 39.790, lng: -104.580 },
          { lat: 39.810, lng: -104.590 }, { lat: 39.795, lng: -104.650 },
          { lat: 39.755, lng: -104.755 }, { lat: 39.700, lng: -104.875 },
          { lat: 39.643, lng: -104.992 }, { lat: 39.592, lng: -105.062 }
        ]
      },
      {
        color: "#ffdd00",
        opacity: 0.35,
        polygon: [
          { lat: 39.605, lng: -105.000 }, { lat: 39.645, lng: -104.912 },
          { lat: 39.692, lng: -104.805 }, { lat: 39.740, lng: -104.688 },
          { lat: 39.778, lng: -104.596 }, { lat: 39.800, lng: -104.590 },
          { lat: 39.786, lng: -104.648 }, { lat: 39.745, lng: -104.758 },
          { lat: 39.696, lng: -104.872 }, { lat: 39.648, lng: -104.978 },
          { lat: 39.614, lng: -105.008 }
        ]
      },
      {
        color: "#ff7700",
        opacity: 0.44,
        polygon: [
          { lat: 39.630, lng: -104.950 }, { lat: 39.668, lng: -104.865 },
          { lat: 39.712, lng: -104.760 }, { lat: 39.758, lng: -104.645 },
          { lat: 39.788, lng: -104.598 }, { lat: 39.778, lng: -104.645 },
          { lat: 39.742, lng: -104.742 }, { lat: 39.696, lng: -104.848 },
          { lat: 39.652, lng: -104.945 }, { lat: 39.626, lng: -104.958 }
        ]
      },
      {
        color: "#ff2200",
        opacity: 0.55,
        polygon: [
          { lat: 39.652, lng: -104.900 }, { lat: 39.688, lng: -104.818 },
          { lat: 39.730, lng: -104.715 }, { lat: 39.768, lng: -104.618 },
          { lat: 39.760, lng: -104.650 }, { lat: 39.722, lng: -104.742 },
          { lat: 39.680, lng: -104.838 }, { lat: 39.645, lng: -104.912 }
        ]
      }
    ]
  }
];
