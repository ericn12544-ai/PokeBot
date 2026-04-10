"""
Module to record drop events to the drop_events.csv file.
"""

import argparse
import csv
import os
from datetime import datetime


def get_next_drop_id(csv_path):
    """Get the next drop_id by reading the existing CSV file."""
    if not os.path.exists(csv_path):
        return 1
    
    max_id = 0
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('drop_id'):
                    max_id = max(max_id, int(row['drop_id']))
    except (FileNotFoundError, ValueError):
        pass
    
    return max_id + 1


def update_drop_products(drop_id, product_ids):
    """Add product associations for a drop to drop_products.csv."""
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'drop_products.csv')
    csv_path = os.path.abspath(csv_path)
    
    # Parse product_ids (comma-separated)
    product_list = [pid.strip() for pid in product_ids.split(',') if pid.strip()]
    
    if not product_list:
        return
    
    # Read existing data
    rows = []
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    
    # Add new product associations
    for product_id in product_list:
        rows.append({
            'drop_id': drop_id,
            'product_id': product_id
        })
    
    # Write back to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['drop_id', 'product_id'])
        writer.writeheader()
        writer.writerows(rows)


def record_drop(retailer, source, zip_code, price, product_ids, notes=""):
    """Record a drop event to the drop_events CSV file."""
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'drop_events.csv')
    csv_path = os.path.abspath(csv_path)
    
    # Get next drop_id
    drop_id = get_next_drop_id(csv_path)
    
    # Create new row
    new_row = {
        'drop_id': drop_id,
        'retailer': retailer,
        'source': source,
        'zip_code': zip_code,
        'price_observed': price,
        'observed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'notes': notes
    }
    
    # Read existing data
    rows = []
    fieldnames = ['drop_id', 'retailer', 'source', 'zip_code', 'price_observed', 'observed_at', 'notes']
    
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    
    # Append new row
    rows.append(new_row)
    
    # Write back to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    # Update drop_products.csv if product_ids provided
    if product_ids:
        update_drop_products(drop_id, product_ids)
    
    print(f"✓ Drop recorded (ID: {drop_id})")
    print(f"  Retailer: {retailer}")
    print(f"  Source: {source}")
    print(f"  Price: ${price}")
    print(f"  Location ZIP: {zip_code}")
    if product_ids:
        print(f"  Products: {product_ids}")
    if notes:
        print(f"  Notes: {notes}")


def main():
    parser = argparse.ArgumentParser(description='Record a drop event')
    parser.add_argument('--retailer', required=True, help='Retailer name')
    parser.add_argument('--source', required=True, help='Source of drop (e.g., in_person, app, online)')
    parser.add_argument('--zip', required=True, help='ZIP code')
    parser.add_argument('--price', required=True, help='Observed price')
    parser.add_argument('--product_ids', required=False, default='', help='Product IDs (comma-separated)')
    parser.add_argument('--notes', required=False, default='', help='Additional notes')
    
    args = parser.parse_args()
    
    record_drop(
        retailer=args.retailer,
        source=args.source,
        zip_code=args.zip,
        price=args.price,
        product_ids=args.product_ids,
        notes=args.notes
    )


if __name__ == '__main__':
    main()
