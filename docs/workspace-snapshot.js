// Recorded synthetic connected runtime evidence.
const snapshot = {
  "cutoff": "2026-06-30",
  "plan": {
    "budget": 3600.0,
    "purchase_cost": 2600.0,
    "solver_status": "OPTIMAL",
    "feasible": true
  },
  "validation": {
    "records": 167,
    "accepted": 163,
    "quarantined": 4,
    "dbt_checks": 189,
    "workflow_checks": 27
  },
  "recommendations": [
    {
      "sku_id": "SKU-001",
      "location_id": "DC-ARG-01",
      "supplier_id": "SUP-001",
      "recommended_quantity": 30,
      "initial_inventory": 84,
      "expected_cost": 224.0,
      "lead_time_days": 2,
      "forecast": [
        [
          "2026-06-30",
          11.0,
          13.0,
          14.0
        ],
        [
          "2026-07-01",
          5.0,
          7.0,
          8.0
        ],
        [
          "2026-07-02",
          9.0,
          11.0,
          12.0
        ],
        [
          "2026-07-03",
          6.0,
          8.0,
          9.0
        ],
        [
          "2026-07-04",
          9.0,
          11.0,
          12.0
        ],
        [
          "2026-07-05",
          5.0,
          7.0,
          8.0
        ],
        [
          "2026-07-06",
          0.0,
          0.0,
          1.0
        ],
        [
          "2026-07-07",
          11.0,
          13.0,
          14.0
        ],
        [
          "2026-07-08",
          5.0,
          7.0,
          8.0
        ],
        [
          "2026-07-09",
          9.0,
          11.0,
          12.0
        ],
        [
          "2026-07-10",
          6.0,
          8.0,
          9.0
        ],
        [
          "2026-07-11",
          9.0,
          11.0,
          12.0
        ],
        [
          "2026-07-12",
          5.0,
          7.0,
          8.0
        ],
        [
          "2026-07-13",
          0.0,
          0.0,
          1.0
        ]
      ],
      "history": {
        "execution_ref": "MOCK-PO-63885F47E51C0F2C",
        "received": 30,
        "demand": 117,
        "events": [
          {
            "event_type": "recommendation_created",
            "actor_role": "planner"
          },
          {
            "event_type": "approved",
            "actor_role": "approver"
          },
          {
            "event_type": "execution_requested",
            "actor_role": "approver"
          },
          {
            "event_type": "executed",
            "actor_role": "system"
          },
          {
            "event_type": "outcome_observed",
            "actor_role": "planner"
          }
        ]
      }
    },
    {
      "sku_id": "SKU-001",
      "location_id": "DC-ARG-02",
      "supplier_id": "SUP-002",
      "recommended_quantity": 50,
      "initial_inventory": 58,
      "expected_cost": 373.0,
      "lead_time_days": 3,
      "forecast": [
        [
          "2026-06-30",
          7.0,
          9.0,
          10.0
        ],
        [
          "2026-07-01",
          7.0,
          9.0,
          10.0
        ],
        [
          "2026-07-02",
          8.0,
          10.0,
          11.0
        ],
        [
          "2026-07-03",
          6.0,
          8.0,
          9.0
        ],
        [
          "2026-07-04",
          9.0,
          11.0,
          12.0
        ],
        [
          "2026-07-05",
          5.0,
          7.0,
          8.0
        ],
        [
          "2026-07-06",
          0.0,
          0.0,
          1.0
        ],
        [
          "2026-07-07",
          7.0,
          9.0,
          10.0
        ],
        [
          "2026-07-08",
          7.0,
          9.0,
          10.0
        ],
        [
          "2026-07-09",
          8.0,
          10.0,
          11.0
        ],
        [
          "2026-07-10",
          6.0,
          8.0,
          9.0
        ],
        [
          "2026-07-11",
          9.0,
          11.0,
          12.0
        ],
        [
          "2026-07-12",
          5.0,
          7.0,
          8.0
        ],
        [
          "2026-07-13",
          0.0,
          0.0,
          1.0
        ]
      ],
      "history": {
        "execution_ref": "MOCK-PO-89B8B3E7C80083BF",
        "received": 50,
        "demand": 101,
        "events": [
          {
            "event_type": "recommendation_created",
            "actor_role": "planner"
          },
          {
            "event_type": "approved",
            "actor_role": "approver"
          },
          {
            "event_type": "execution_requested",
            "actor_role": "approver"
          },
          {
            "event_type": "executed",
            "actor_role": "system"
          },
          {
            "event_type": "outcome_observed",
            "actor_role": "planner"
          }
        ]
      }
    },
    {
      "sku_id": "SKU-002",
      "location_id": "DC-ARG-01",
      "supplier_id": "SUP-002",
      "recommended_quantity": 100,
      "initial_inventory": 16,
      "expected_cost": 992.0,
      "lead_time_days": 2,
      "forecast": [
        [
          "2026-06-30",
          9.0,
          10.0,
          11.0
        ],
        [
          "2026-07-01",
          8.0,
          9.0,
          10.0
        ],
        [
          "2026-07-02",
          11.0,
          12.0,
          13.0
        ],
        [
          "2026-07-03",
          10.0,
          11.0,
          12.0
        ],
        [
          "2026-07-04",
          11.0,
          12.0,
          13.0
        ],
        [
          "2026-07-05",
          8.0,
          9.0,
          10.0
        ],
        [
          "2026-07-06",
          0.0,
          0.0,
          1.0
        ],
        [
          "2026-07-07",
          9.0,
          10.0,
          11.0
        ],
        [
          "2026-07-08",
          8.0,
          9.0,
          10.0
        ],
        [
          "2026-07-09",
          11.0,
          12.0,
          13.0
        ],
        [
          "2026-07-10",
          10.0,
          11.0,
          12.0
        ],
        [
          "2026-07-11",
          11.0,
          12.0,
          13.0
        ],
        [
          "2026-07-12",
          8.0,
          9.0,
          10.0
        ],
        [
          "2026-07-13",
          0.0,
          0.0,
          1.0
        ]
      ],
      "history": {
        "execution_ref": "MOCK-PO-06AB329B48597B80",
        "received": 100,
        "demand": 124,
        "events": [
          {
            "event_type": "recommendation_created",
            "actor_role": "planner"
          },
          {
            "event_type": "approved",
            "actor_role": "approver"
          },
          {
            "event_type": "execution_requested",
            "actor_role": "approver"
          },
          {
            "event_type": "executed",
            "actor_role": "system"
          },
          {
            "event_type": "outcome_observed",
            "actor_role": "planner"
          }
        ]
      }
    },
    {
      "sku_id": "SKU-002",
      "location_id": "DC-ARG-02",
      "supplier_id": "SUP-001",
      "recommended_quantity": 80,
      "initial_inventory": 20,
      "expected_cost": 1011.0,
      "lead_time_days": 2,
      "forecast": [
        [
          "2026-06-30",
          9.0,
          11.0,
          13.0
        ],
        [
          "2026-07-01",
          9.0,
          11.0,
          13.0
        ],
        [
          "2026-07-02",
          10.0,
          12.0,
          14.0
        ],
        [
          "2026-07-03",
          10.0,
          12.0,
          14.0
        ],
        [
          "2026-07-04",
          13.0,
          15.0,
          17.0
        ],
        [
          "2026-07-05",
          6.0,
          8.0,
          10.0
        ],
        [
          "2026-07-06",
          0.0,
          0.0,
          2.0
        ],
        [
          "2026-07-07",
          9.0,
          11.0,
          13.0
        ],
        [
          "2026-07-08",
          9.0,
          11.0,
          13.0
        ],
        [
          "2026-07-09",
          10.0,
          12.0,
          14.0
        ],
        [
          "2026-07-10",
          10.0,
          12.0,
          14.0
        ],
        [
          "2026-07-11",
          13.0,
          15.0,
          17.0
        ],
        [
          "2026-07-12",
          6.0,
          8.0,
          10.0
        ],
        [
          "2026-07-13",
          0.0,
          0.0,
          2.0
        ]
      ],
      "history": {
        "execution_ref": "MOCK-PO-4169DEADEBE33F29",
        "received": 80,
        "demand": 142,
        "events": [
          {
            "event_type": "recommendation_created",
            "actor_role": "planner"
          },
          {
            "event_type": "approved",
            "actor_role": "approver"
          },
          {
            "event_type": "execution_requested",
            "actor_role": "approver"
          },
          {
            "event_type": "executed",
            "actor_role": "system"
          },
          {
            "event_type": "outcome_observed",
            "actor_role": "planner"
          }
        ]
      }
    }
  ],
  "scenarios": [
    {
      "name": "Normal",
      "expected_cost": 3883.93,
      "fill_rate": 1.0,
      "expedite_units": 50.333333333333336,
      "evaluation_paths": 6
    },
    {
      "name": "Demand Surge",
      "expected_cost": 6690.98,
      "fill_rate": 1.0,
      "expedite_units": 211.16666666666666,
      "evaluation_paths": 6
    },
    {
      "name": "Supplier Delay",
      "expected_cost": 4147.586666666667,
      "fill_rate": 1.0,
      "expedite_units": 66.33333333333333,
      "evaluation_paths": 6
    }
  ]
};
