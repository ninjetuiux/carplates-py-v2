import csv
import random
from datetime import datetime, timedelta
import sqlite3
from difflib import SequenceMatcher

def generate_random_license_plate():
    """
    Generates a random Israeli-format license plate with potential errors.
    
    Debug Steps:
    1. Choose plate length (7 or 8 chars)
    2. Generate base plate
    3. Apply random error (if triggered)
    4. Return modified or original plate
    """
    # Define possible characters (excluding I,O to prevent confusion)
    characters = '0123456789ABCDEFGHJKLMNPQRSTUVWXYZ'
    
    # Choose random length (7 or 8 chars)
    length = random.choice([7, 8])
    print(f"DEBUG: Generating plate with length {length}")
    
    # Generate base plate
    plate = ''.join(random.choices(characters, k=length))
    print(f"DEBUG: Base plate generated: {plate}")
    
    # 20% chance for introducing errors
    if random.random() < 0.2:
        deviation_type = random.choice(['swap', 'missing_start', 'missing_end', 'missing_multiple'])
        print(f"DEBUG: Applying error type: {deviation_type}")
        
        if deviation_type == 'swap':
            # Common character replacements
            replacements = [
                ('3', '8'), ('8', '3'),  # Similar digits
                ('1', '7'), ('7', '1'),
                ('0', '8'), ('0', '3'),
                ('B', '8'), ('S', '5'),  # Similar letters
                ('Z', '2'), ('2', 'Z'),
                ('O', '0'), ('I', '1')
            ]
            for old, new in replacements:
                if old in plate and random.random() < 0.3:
                    plate = plate.replace(old, new)
                    print(f"DEBUG: Swapped {old} with {new}")
        
        elif deviation_type in ['missing_start', 'missing_end']:
            plate = plate[1:] if deviation_type == 'missing_start' else plate[:-1]
            print(f"DEBUG: Removed character from {deviation_type}")
        
        elif deviation_type == 'missing_multiple':
            num_chars_to_remove = random.randint(1, 2)
            positions_to_remove = random.sample(range(len(plate)), num_chars_to_remove)
            plate = ''.join(char for idx, char in enumerate(plate) if idx not in positions_to_remove)
            print(f"DEBUG: Removed {num_chars_to_remove} characters from positions {positions_to_remove}")

    print(f"DEBUG: Final plate: {plate}")
    return plate

def create_csv(file_name, num_records=100):
    """
    Creates a CSV file with timestamp-plate pairs, including deliberate errors.
    
    Args:
        file_name (str): Output CSV file name
        num_records (int): Number of records to generate
    
    Debug Steps:
    1. Initialize CSV file
    2. Generate plates in pairs (original + error)
    3. Write records with timestamps
    """
    print(f"\nDEBUG: Starting CSV creation with {num_records} records")
    start_time = datetime.now()
    
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['DateTime', 'LicensePlate'])
        print("DEBUG: CSV header written")
        
        # Generate plates in pairs
        for pair_num in range(num_records // 2):
            print(f"\nDEBUG: Generating pair {pair_num + 1}")
            
            # Generate base plate
            base_plate = ''.join(random.choices('0123456789ABCDEFGHJKLMNPQRSTUVWXYZ', k=7))
            error_plate = base_plate
            
            # Create error version
            error_type = random.choice(['swap', 'remove'])
            print(f"DEBUG: Error type chosen: {error_type}")
            
            if error_type == 'swap':
                replacements = {
                    '0': '8', '1': '7', '2': 'Z', '3': '8',
                    '5': 'S', '6': 'G', '8': '3', 'B': '8',
                    'S': '5', 'Z': '2'
                }
                
                possible_chars = [i for i, c in enumerate(error_plate) if c in replacements]
                if possible_chars:
                    pos = random.choice(possible_chars)
                    char = error_plate[pos]
                    error_plate = error_plate[:pos] + replacements[char] + error_plate[pos+1:]
                    print(f"DEBUG: Swapped character at position {pos}: {char} -> {replacements[char]}")
            else:
                pos = random.randint(0, len(error_plate)-1)
                error_plate = error_plate[:pos] + error_plate[pos+1:]
                print(f"DEBUG: Removed character at position {pos}")
            
            # Write original plate
            timestamp1 = start_time.strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp1, base_plate])
            print(f"DEBUG: Written original: {timestamp1} - {base_plate}")
            
            # Write error plate after 20 seconds
            start_time += timedelta(seconds=20)
            timestamp2 = start_time.strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp2, error_plate])
            print(f"DEBUG: Written modified: {timestamp2} - {error_plate}")
            
            # Add gap before next pair
            start_time += timedelta(minutes=1)
            print("-" * 50)
    
    print(f"\nCSV file '{file_name}' created with {num_records} records.")

def create_table(db_name='database.db'):
    """
    Creates the necessary database tables for license plate tracking.
    
    Args:
        db_name (str): Database file name
    
    Debug Steps:
    1. Connect to SQLite database
    2. Create main license_plates table if not exists
    3. Create corrected_plates table if not exists
    4. Commit and close connection
    """
    print(f"\nDEBUG: Creating database tables in {db_name}")
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create main table for storing license plate readings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS license_plates (
            id INTEGER PRIMARY KEY,
            DateTime TEXT,
            LicensePlate TEXT
        )
    ''')
    print("DEBUG: Main table 'license_plates' created or verified")
    
    # Drop existing corrected_plates table if exists
    cursor.execute('DROP TABLE IF EXISTS corrected_plates')
    
    # Create corrected_plates table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS corrected_plates (
            id INTEGER PRIMARY KEY,
            original_id INTEGER,
            timestamp TEXT,
            original_plate TEXT,
            corrected_plate TEXT,
            confidence_score INTEGER,
            correction_type TEXT,
            FOREIGN KEY (original_id) REFERENCES license_plates (id)
        )
    ''')
    print("DEBUG: Corrected plates table created or verified")
    
    conn.commit()
    conn.close()
    print("DEBUG: Database connection closed")

def load_csv_to_db(csv_file, db_name='database.db'):
    """
    Loads license plate data from CSV file into database.
    
    Args:
        csv_file (str): Input CSV file path
        db_name (str): Database file name
    
    Debug Steps:
    1. Open database connection
    2. Read CSV file line by line
    3. Insert each record into database
    4. Commit and close connection
    """
    print(f"\nDEBUG: Starting data load from {csv_file} to {db_name}")
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    records_count = 0

    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute('''
                INSERT INTO license_plates (DateTime, LicensePlate)
                VALUES (?, ?)
            ''', (row['DateTime'], row['LicensePlate']))
            records_count += 1
            
            if records_count % 10 == 0:  # Progress update every 10 records
                print(f"DEBUG: Processed {records_count} records")
    
    conn.commit()
    conn.close()
    print(f"DEBUG: Completed loading {records_count} records into database")

def find_license_plate_errors(time_window_minutes=5, similarity_threshold=70, db_name='database.db'):
    """
    Identifies potential errors in license plate readings by comparing similar plates within a time window.
    
    Args:
        time_window_minutes (int): Time window for comparing plates (default: 5)
        similarity_threshold (int): Minimum similarity percentage (default: 70)
        db_name (str): Database file name
    
    Debug Steps:
    1. Retrieve all records ordered by time
    2. Compare each plate with subsequent plates within time window
    3. Calculate similarity scores
    4. Collect potential errors
    5. Generate detailed report
    
    Returns:
        list: Dictionary of identified errors with details
    """
    print(f"\nDEBUG: Starting error detection (window: {time_window_minutes}m, threshold: {similarity_threshold}%)")
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Get all records ordered by time
    cursor.execute('SELECT id, DateTime, LicensePlate FROM license_plates ORDER BY DateTime')
    records = cursor.fetchall()
    print(f"DEBUG: Retrieved {len(records)} records for processing")

    errors = []
    time_window = timedelta(minutes=time_window_minutes)
    comparisons_count = 0

    for i, record1 in enumerate(records):
        id1, time1, plate1 = record1
        time1 = datetime.strptime(time1, '%Y-%m-%d %H:%M:%S')
        
        # Compare with next few records within time window
        for record2 in records[i+1:i+5]:  # Limit to next 4 records for efficiency
            id2, time2, plate2 = record2
            time2 = datetime.strptime(time2, '%Y-%m-%d %H:%M:%S')
            
            time_diff = time2 - time1
            if time_diff > time_window:
                break
                
            if plate1 != plate2:  # Skip identical plates
                similarity = SequenceMatcher(None, plate1, plate2).ratio() * 100
                comparisons_count += 1
                
                if similarity >= similarity_threshold:
                    errors.append({
                        'id1': id1,
                        'id2': id2,
                        'time1': time1.strftime('%Y-%m-%d %H:%M:%S'),
                        'time2': time2.strftime('%Y-%m-%d %H:%M:%S'),
                        'plate1': plate1,
                        'plate2': plate2,
                        'similarity': similarity
                    })
                    print(f"DEBUG: Found potential error - {plate1} vs {plate2} = {similarity:.1f}%")

    conn.close()
    print(f"DEBUG: Completed {comparisons_count} comparisons, found {len(errors)} potential errors")
    
    # Generate detailed report if errors found
    if errors:
        print("\nDetailed Error Report:")
        print("-" * 100)
        print(f"{'First Time':<20} {'Plate 1':<10} {'Second Time':<20} {'Plate 2':<10} {'Similarity':<10}")
        print("-" * 100)
        for error in errors:
            print(f"{error['time1']:<20} {error['plate1']:<10} "
                  f"{error['time2']:<20} {error['plate2']:<10} "
                  f"{error['similarity']:.1f}%")

    return errors

def update_license_plates_with_scores(errors, db_name='database.db'):
    """
    Updates database with corrected license plates and confidence scores.
    
    Args:
        errors (list): List of identified errors and their details
        db_name (str): Database file name
    
    Debug Steps:
    1. Create corrected_plates table if not exists
    2. Process each error:
        - Determine correction type
        - Select corrected version
        - Calculate confidence score
    3. Insert corrections into database
    4. Generate correction summary report
    """
    print("\nDEBUG: Starting correction process")
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create or clear corrected_plates table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS corrected_plates (
            id INTEGER PRIMARY KEY,
            original_id INTEGER,
            timestamp TEXT,
            original_plate TEXT,
            corrected_plate TEXT,
            confidence_score INTEGER,
            correction_type TEXT,
            FOREIGN KEY (original_id) REFERENCES license_plates (id)
        )
    ''')
    cursor.execute('DELETE FROM corrected_plates')
    print("DEBUG: Prepared corrected_plates table")

    # Process each error and insert corrections
    for error in errors:
        # Determine correction type and corrected plate
        original = error['plate1']
        candidate = error['plate2']
        
        correction_type = 'unknown'
        if len(original) > len(candidate):
            correction_type = 'missing_chars'
        elif len(original) < len(candidate):
            correction_type = 'added_chars'
        else:
            correction_type = 'char_swap'

        # Choose longer plate as corrected version
        corrected = original if len(original) >= len(candidate) else candidate
        
        cursor.execute('''
            INSERT INTO corrected_plates (
                original_id, timestamp, original_plate, 
                corrected_plate, confidence_score, correction_type
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            error['id1'],
            error['time1'],
            original,
            corrected,
            int(error['similarity']),
            correction_type
        ))
        print(f"DEBUG: Processed correction: {original} -> {corrected} ({correction_type})")

    conn.commit()

    # Generate correction summary
    print("\nCorrection Summary:")
    cursor.execute('''
        SELECT correction_type, COUNT(*), AVG(confidence_score)
        FROM corrected_plates
        GROUP BY correction_type
    ''')
    print("-" * 60)
    print(f"{'Correction Type':<20} {'Count':<10} {'Avg Confidence':<15}")
    print("-" * 60)
    for correction_type, count, avg_score in cursor.fetchall():
        print(f"{correction_type:<20} {count:<10} {avg_score:.1f}%")

    conn.close()
    print(f"\nDEBUG: Completed processing {len(errors)} corrections")

# Main execution block
if __name__ == "__main__":
    print("DEBUG: Starting license plate processing")
    
    # Create CSV file with test data
    create_csv('license_plates.csv', num_records=100)
    
    # Initialize database
    create_table()
    
    # Load data from CSV
    load_csv_to_db('license_plates.csv')
    
    # Find and process errors
    errors = find_license_plate_errors(
        time_window_minutes=5,
        similarity_threshold=70,
        db_name='database.db'
    )
    
    # Update corrections if errors found
    if errors:
        update_license_plates_with_scores(errors)
    
    print("DEBUG: Processing completed")
