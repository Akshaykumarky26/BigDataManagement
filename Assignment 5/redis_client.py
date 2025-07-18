import redis
import csv
import re
import os

from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.query import Query
from redis.commands.search.indexDefinition import IndexDefinition, IndexType


class Redis_Client:
    def __init__(self):
        self.r = None
        self.redis_host = 'localhost'
        self.redis_port = 6379
        self.redis_db = 0
        self.redis_password = None
        self.search_client = None

    def connect(self):
        print(f"Attempting to connect to Redis at {self.redis_host}:{self.redis_port}...")
        try:
            self.r = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10
            )
            self.r.ping()
            print("Successfully connected to Redis!")
            self.search_client = self.r.ft('idx:users')
        except redis.exceptions.ConnectionError as e:
            print(f"Could not connect to Redis: {e}")
            self.r = None
            self.search_client = None
        except Exception as e:
            print(f"An unexpected error occurred during Redis connection: {e}")
            self.r = None
            self.search_client = None

    def close(self):
        """
        Closes the Redis connection.
        """
        if self.r:
            self.r.close()
            print("Redis connection closed.")

    def load_users(self, file_path="users.txt"):
        """
        Loads user data from the specified file into Redis Hashes.
        Converts longitude and latitude to float.
        """
        if not self.r:
            print("Not connected to Redis. Please connect first.")
            return

        print(f"Loading user data from {file_path}...")
        user_count = 0
        try:
            if not os.path.exists(file_path):
                print(f"Error: User data file '{file_path}' not found at {os.path.abspath(file_path)}.")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    parts = re.findall(r'"([^"]*)"|\S+', line)

                    user_key = parts[0].strip('"')

                    user_data = {}
                    for i in range(1, len(parts), 2):
                        if i + 1 < len(parts):
                            key = parts[i].strip('"')
                            value = parts[i+1].strip('"')
                            
                            if key in ['longitude', 'latitude']:
                                try:
                                    user_data[key] = float(value)
                                except ValueError:
                                    print(f"WARNING: Could not convert {key} '{value}' to float for {user_key}. Storing as string.")
                                    user_data[key] = value
                            else:
                                user_data[key] = value

                    self.r.hset(user_key, mapping=user_data)
                    user_count += 1

            print(f"Successfully loaded {user_count} users into Redis.")

        except Exception as e:
            print(f"An error occurred while loading users: {e}")

    def load_scores(self, file_path="userscores.csv"):
        """
        Loads score data from the specified CSV file into Redis Sorted Sets.
        """
        if not self.r:
            print("Not connected to Redis. Please connect first.")
            return

        print(f"Loading score data from {file_path}...")
        if not os.path.exists(file_path):
            print(f"Error: Score data file '{file_path}' not found at {os.path.abspath(file_path)}.")
            return

        score_count = 0
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for i, row in enumerate(reader):
                    cleaned_row = {}
                    for k, v in row.items():
                        cleaned_key = k.strip() if k is not None else ''
                        cleaned_value = v.strip() if v is not None else ''
                        cleaned_row[cleaned_key] = cleaned_value

                    user_id = cleaned_row.get('user:id')
                    score_str = cleaned_row.get('score')
                    leaderboard_name = cleaned_row.get('leaderboard')

                    if not user_id or not score_str or not leaderboard_name:
                        continue

                    try:
                        score = int(score_str)
                    except ValueError:
                        print(f"Error: Line {i+2}: Cannot convert score '{score_str}' to integer for user {user_id}.")
                        print(f"Problematic row (raw from DictReader): {row}")
                        print(f"Problematic row (after stripping & cleaning): {cleaned_row}")
                        raise

                    self.r.zadd(leaderboard_name, {user_id: score})
                    score_count += 1
            print(f"Successfully loaded {score_count} scores into Redis.")

        except Exception as e:
            print(f"An unexpected error occurred while loading scores: {e}")


    def query1(self, user_id):
        """
        Returns all attributes of the user by user ID.
        """
        if not self.r:
            print("Not connected to Redis. Please connect first.")
            return None

        print(f"Executing query1: Retrieving all attributes for {user_id}...")
        try:
            user_attributes = self.r.hgetall(user_id)
            if user_attributes:
                print(f"Found attributes for {user_id}:")
                for key, value in user_attributes.items():
                    print(f"  {key}: {value}")
                return user_attributes
            else:
                print(f"User '{user_id}' not found in Redis.")
                return None
        except Exception as e:
            print(f"An error occurred during query1 for {user_id}: {e}")
            return None

    def query2(self, user_id):
        """
        Returns the coordinate (longitude and latitude) of the user by the user ID.
        """
        if not self.r:
            print("Not connected to Redis. Please connect first.")
            return None

        print(f"Executing query2: Retrieving coordinates for {user_id}...")
        try:
            # HMGET retrieves the values associated with the specified fields in a hash.
            # It returns a list of values in the same order as the requested fields.
            coordinates = self.r.hmget(user_id, 'longitude', 'latitude')
            
            longitude = coordinates[0]
            latitude = coordinates[1] 

            if longitude is not None and latitude is not None:
                # Convert to float for consistency, as they might be strings if conversion failed in load_users
                try:
                    longitude = float(longitude)
                    latitude = float(latitude)
                    print(f"Coordinates for {user_id}: Longitude={longitude}, Latitude={latitude}")
                    return {"longitude": longitude, "latitude": latitude}
                except ValueError:
                    print(f"Error: Could not convert coordinates to float for {user_id}. Stored values: Longitude='{longitude}', Latitude='{latitude}'")
                    return None
            else:
                # This case handles if user_id exists but one of the fields is missing.
                # Or if user_id does not exist, hmget returns [None, None]
                if self.r.exists(user_id): # Check if the user key itself exists
                    print(f"User '{user_id}' found, but longitude/latitude are missing or malformed.")
                else:
                    print(f"User '{user_id}' not found in Redis.")
                return None
        except Exception as e:
            print(f"An error occurred during query2 for {user_id}: {e}")
            return None
            
    def query3(self):
        """
        Gets the keys and last names of the users whose IDs do not start with an odd number.
        Searching for the keyspace starts at cursor 1280.
        """
        if not self.r:
            print("Not connected to Redis. Please connect first.")
            return [], []

        print("Executing query3: Getting user keys and last names (IDs not starting with odd numbers)...")
        
        # Use a set to automatically handle duplicates from SCAN's partial iterations
        unique_matching_user_keys = set()
        
        cursor = '1280' # Cursor needs to be a string for redis-py
        count = 100 # Number of elements per call (can be adjusted)
        
        # Add a safety counter to prevent infinite loops in case SCAN never returns '0'
        # For 6000 users, 100 keys per call, should take ~60 iterations. 1000 is a generous limit.
        max_scan_iterations = 1000
        current_iteration_count = 0
        
        try:
            while True:
                current_iteration_count += 1
                if current_iteration_count > max_scan_iterations:
                    print(f"WARNING: query3 SCAN exceeded {max_scan_iterations} iterations. Returning results found so far.")
                    break # Break if safety limit reached

                cursor, keys = self.r.scan(cursor=cursor, match="user:*", count=100)
                
                for key in keys:
                    # Extract the numerical part of the user ID (e.g., "123" from "user:123")
                    try:
                        user_id_num_str = key.split(':')[1]
                        if not user_id_num_str: # Skip if ID part is empty (e.g., "user:")
                            continue
                        
                        first_digit = user_id_num_str[0] # Get the first digit of the number
                        
                        # Check if the first digit is a digit and is NOT odd (i.e., it's even: 0, 2, 4, 6, 8)
                        if first_digit.isdigit() and first_digit in ['0', '2', '4', '6', '8']:
                            unique_matching_user_keys.add(key) # Add to set (handles duplicates)
                    except IndexError: # Handles keys not in "user:ID" format
                        continue
                    except ValueError: # Handles cases where user_id_num_str[0] is not a digit
                        continue
                
                if cursor == '0': # Break if Redis indicates end of scan
                    break
            
            # After the scan loop, build the results lists from the unique set
            result_user_ids = sorted(list(unique_matching_user_keys)) # Convert set to list and sort for consistent order
            result_last_names = []
            for user_key in result_user_ids:
                last_name = self.r.hget(user_key, 'last_name') # Fetch last_name for each unique user
                if last_name:
                    result_last_names.append(last_name)
                else:
                    result_last_names.append(None) # Append None if last_name is missing
 
            print(f"Query3 complete. Found {len(result_user_ids)} matching users.")
            print("Sample of Query 3 results (first 5):")
            for uid, lastname in zip(result_user_ids[:5], result_last_names[:5]):
                print(f"  {uid}: {lastname}")
 
            return result_user_ids, result_last_names # Return the filtered results
 
        except Exception as e:
            print(f"An error occurred during query3: {e}")
            return [], []

    def create_user_index(self):
        """
        Creates a secondary index in Redisearch for user data.
        Specification: gender(text), country(tag), latitude(Numeric), first_name(text).
        """
        if not self.r or not self.search_client:
            print("Not connected to Redis or Redisearch client not initialized.")
            return

        index_name = "idx:users" # Our chosen index name
        
        try:
            # Drop index if it already exists (useful for development, ensures fresh index)
            try:
                self.search_client.info()
                print(f"Index '{index_name}' already exists. Dropping existing index.")
                self.search_client.dropindex()
            except:
                print(f"Index '{index_name}' does not exist or cannot be accessed (proceeding to create).")
            
            # Define the schema for the index as per assignment specification
            schema = (
                TextField("gender"),
                TagField("country"),
                NumericField("latitude"),
                TextField("first_name"),
            )
            
            # Define the index on Redis Hashes with "user:" prefix
            definition = IndexDefinition(prefix=["user:"], index_type=IndexType.HASH)
            
            self.search_client.create_index(fields=schema, definition=definition)
            print(f"Secondary index '{index_name}' created successfully.")
            
        except Exception as e:
            print(f"Error creating secondary index '{index_name}': {e}")

    def query4(self):
        """
        Returns females in China or Russia with latitude between 40 and 46.
        Combines RediSearch query with a manual SCAN fallback for robustness.
        """
        if not self.r: # Check general Redis connection
            print("Not connected to Redis. Please connect first.")
            return []

        print("Executing query4: Finding females in China or Russia with latitude between 40 and 46...")
        
        users_info = [] # List to store matching user dictionaries

        # --- Attempt RediSearch query first ---
        if self.search_client: # Check if Redisearch client is initialized
            try:
                # Construct the RediSearch query string based on criteria
                query_string = "@gender:{female} ((@country:{China}) | (@country:{Russia})) @latitude:[40 46]"
                q = Query(query_string)
                
                # Execute the search query using the RediSearch client
                result = self.search_client.search(q) 
                
                print(f"RediSearch: Found {result.total} matching documents.")
                
                if result.total > 0:
                    print("RediSearch Sample of Query 4 results (first 5):")
                    for i, doc in enumerate(result.docs):
                        if i >= 5: # Limit sample output to first 5
                            break
                        user_info = { 
                            'id': doc.id, # The Redis key (e.g., user:123)
                            'first_name': getattr(doc, 'first_name', ''), # Get indexed first_name
                            'last_name': '', # Initialize last_name
                            'country': getattr(doc, 'country', ''), # Get indexed country
                            'latitude': getattr(doc, 'latitude', ''), # Get indexed latitude
                            'email': '' # Initialize email
                        } 
                        
                        # Fetch last_name and email explicitly from hash, as they are not in the index schema
                        # This ensures all required fields are present if they are needed for full user info.
                        full_user_data = self.r.hgetall(doc.id)
                        user_info['last_name'] = full_user_data.get('last_name', '')
                        user_info['email'] = full_user_data.get('email', '')

                        users_info.append(user_info)
                        print(f"  {user_info['id']}: {user_info['first_name']} {user_info['last_name']} from {user_info['country']} (lat: {user_info['latitude']})") 
                    
                    return users_info # Return results if RediSearch found any
                else:
                    print("RediSearch: No matching users found. Falling back to manual search (for completeness).")
            
            except Exception as search_error: 
                print(f"RediSearch query failed: {search_error}") 
                print("Falling back to manual search due to RediSearch error.") 

        # --- Fallback method using SCAN and manual filtering ---
        print("Using manual search method for query4...") 
        
        cursor = '0' # Start from cursor '0' for a complete manual scan
        count = 100 # Hint for elements per call 
        
        max_scan_iterations = 1000 # Safety counter for manual SCAN
        current_iteration_count = 0
        
        while True: 
            current_iteration_count += 1
            if current_iteration_count > max_scan_iterations:
                print(f"WARNING: Manual SCAN for query4 exceeded {max_scan_iterations} iterations. Returning partial results from manual scan.")
                break

            cursor, keys = self.r.scan(cursor=cursor, match="user:*", count=count) 
            
            for key in keys: 
                user_data = self.r.hgetall(key) # Get all attributes of the user hash
                
                # Retrieve and clean data for filtering
                gender = user_data.get('gender')
                country = user_data.get('country')
                latitude_str = user_data.get('latitude') # Latitude as a string initially

                if (gender == 'female' and 
                        country in ['China', 'Russia'] and # Check country
                        latitude_str): # Ensure latitude_str is not None or empty
                    try: 
                        lat = float(latitude_str) # Convert latitude to float for numerical comparison
                        if 40 <= lat <= 46: # Check latitude range
                            user_info = { 
                                'id': key, 
                                'first_name': user_data.get('first_name', ''), 
                                'last_name': user_data.get('last_name', ''), 
                                'country': user_data.get('country', ''), 
                                'latitude': user_data.get('latitude', ''), 
                                'email': user_data.get('email', '') 
                            } 
                            users_info.append(user_info) 
                            # Print result immediately for manual search for debugging visibility
                            print(f"  {user_info['id']}: {user_info['first_name']} {user_info['last_name']} from {user_info['country']} (lat: {user_info['latitude']}) (Manual Search)") 
                    except ValueError: 
                        # Continue if latitude cannot be converted to float (skip this user for query4)
                        continue 
            
            if cursor == '0': # Break if Redis indicates end of scan
                break 
        
        print(f"Found {len(users_info)} female users in China or Russia with latitude 40-46 (via manual search).") 
        return users_info 

    def query5(self):
        """
        Gets the email IDs of the top 10 players (in terms of score) in leaderboard:2.
        """
        if not self.r:
            print("Not connected to Redis. Please connect first.")
            return []

        print("Executing query5: Getting email IDs of top 10 players in leaderboard:2...")
        
        leaderboard_name = "leaderboard:2"
        email_ids = []
        
        try:
            # ZREVRANGE returns members in descending order by score (highest first)
            # 0 to 9 means the first 10 members (0-indexed)
            top_10_users_with_scores = self.r.zrevrange(leaderboard_name, 0, 9, withscores=True)
            
            if not top_10_users_with_scores:
                print(f"Leaderboard '{leaderboard_name}' is empty or not found.")
                return []
                
            print(f"Top 10 players in {leaderboard_name}:")
            for i, (user_id, score) in enumerate(top_10_users_with_scores):
                # For each user_id, retrieve their email from the user hash
                email = self.r.hget(user_id, 'email')
                
                if email:
                    email_ids.append(email)
                    print(f"  {i+1}. {user_id} (Score: {int(score)}) - Email: {email}")
                else:
                    print(f"  {i+1}. {user_id} (Score: {int(score)}) - Email: Not found.")
                    email_ids.append(None) # Append None if email not found
            
            return email_ids

        except Exception as e:
            print(f"An error occurred during query5: {e}")
            return []


if __name__ == "__main__":
    rs = Redis_Client()
    rs.connect()

    if rs.r:
        print("Clearing all existing data in Redis (using FLUSHDB) for a clean load...")
        try:
            rs.r.flushdb()
            print("Redis database cleared.")
        except Exception as e:
            print(f"Error clearing Redis database: {e}")

        rs.load_users("users.txt") 
        rs.load_scores("userscores.csv")

        print("\n--- Testing leaderboard:2 ---")
        leaderboard_id_to_check = "leaderboard:2"
        top_players = rs.r.zrevrange(leaderboard_id_to_check, 0, 4, withscores=True)
        if top_players:
            print(f"Top 5 players in {leaderboard_id_to_check}:")
            for i, (member, score) in enumerate(top_players):
                print(f"  {i+1}. {member} (Score: {int(score)})")
        else:
            print(f"Leaderboard '{leaderboard_id_to_check}' not found or empty.")

        print("\n--- Testing query1 ---")
        rs.query1("user:1")
        
        print("\n--- Testing query2 ---")
        rs.query2("user:1")
        rs.query2("user:2")
        
        # --- Test query3 ---
        print("\n--- Testing query3 ---")
        rs.close() # Close connection after all non-Redisearch tests
        rs.connect() # Re-establish a fresh connection for query3 (SCAN issues)
        
        user_ids_q3, last_names_q3 = rs.query3()
        print(f"Query3 complete. Found {len(user_ids_q3)} matching users.")
        if user_ids_q3:
            print("Sample of Query 3 results (first 5):")
            for i in range(min(5, len(user_ids_q3))):
                print(f"  {user_ids_q3[i]}: {last_names_q3[i]}")
        else:
            print("No matching users found in query3 sample.")

        # --- Test query4 (Redisearch with Fallback) ---
        print("\n--- Testing query4 (Redisearch with Fallback) ---")
        rs.close() # Close connection after query3
        rs.connect() # Re-establish connection for Redisearch

        # Flushdb and reload users to ensure data is correctly typed before index creation
        try:
            rs.r.flushdb()
            print("Redis database re-cleared before re-loading users for Redisearch index.")
        except Exception as e:
            print(f"Error re-clearing Redis database: {e}")
        rs.load_users("users.txt") # Re-load users with float conversion
        
        rs.create_user_index() # Create the index after data is loaded correctly

        query4_results = rs.query4() # Execute query4 (which includes RediSearch and Fallback)
        if query4_results:
            print(f"Total matching users from Query 4 (returned from method): {len(query4_results)}")
        else:
            print("No matching users returned from Query 4 method.")

        # --- Test query5 ---
        print("\n--- Testing query5 ---")
        rs.close() # Close connection after query4
        rs.connect() # Re-establish connection for query5
        
        # FIX: Need to re-load scores here as database was flushed for query4's setup.
        rs.load_scores("userscores.csv") # <--- ADDED THIS LINE

        top_10_emails = rs.query5()
        print(f"Total email IDs retrieved for top 10 players: {len(top_10_emails)}")
        # The printing of sample results is handled inside query5 method

    else:
        print("Connection failed, skipping data loading and test operations.")

    rs.close()