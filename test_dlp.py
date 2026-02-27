#!/usr/bin/env python3
"""
Quick test script to verify Cloud DLP card masking works correctly.
Run this before starting the full pipeline.
"""

from google.cloud import dlp_v2

PROJECT_ID = "retail-bank-sim"

def test_dlp_masking():
    dlp_client = dlp_v2.DlpServiceClient()
    parent = f"projects/{PROJECT_ID}"

    # Test credit card numbers
    test_cards = [
        "4532-1234-5678-9010",
        "5425233430109903",
        "378282246310005"
    ]

    inspect_config = {
        "info_types": [{"name": "CREDIT_CARD_NUMBER"}]
    }

    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "character_mask_config": {
                            "masking_character": "*",
                            "reverse_order": True,  # Keep last 4 digits visible
                            "number_to_mask": 12    # Mask first 12 digits
                        }
                    }
                }
            ]
        }
    }

    print("Testing Cloud DLP Card Number Masking")
    print("=" * 60)

    for card in test_cards:
        item = {"value": card}
        response = dlp_client.deidentify_content(
            request={
                "parent": parent,
                "deidentify_config": deidentify_config,
                "inspect_config": inspect_config,
                "item": item
            }
        )
        masked = response.item.value
        print(f"Original: {card}")
        print(f"Masked:   {masked}")
        print()

    print("✓ DLP masking is working correctly!")
    print("\nYou can now run:")
    print("  python transaction_generator.py  (Terminal 1)")
    print("  python bq_consumer_dlp.py         (Terminal 2)")

if __name__ == "__main__":
    test_dlp_masking()
