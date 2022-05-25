import sqlite3
from dataclasses import dataclass

# Data class for a site option entity
@dataclass
class SiteOption:
	site_id: int
	brand: str
	pn: str
	dp_id: int
	on_site: bool

# Create schema if not already active
def create_tables(con):
	# Create a table to store item option data and fills
	con.execute('''
	CREATE TABLE IF NOT EXISTS site_options(
		-- Lookup columns
		-- ROWID is automatic in SQLite
		site_id integer not null,
		brand string not null,
		pn string not null,
		dp_id integer not null,
		-- Data columns, these need to be nullable for our fill scheme to work
		-- SQLite doesn't have boolean types
		on_site integer default true);
	''')
	# Create a unique index over the lookup values. 
	# This allows the insert-or-replace commands to work and ensure fills don't overlap.
	con.execute('''
	CREATE UNIQUE INDEX IF NOT EXISTS idx_site_options ON site_options(site_id, brand, pn, dp_id);
	''')
	con.commit()

# Fetch a single site option from the database
def fetch_site_option(con, site_id, brand, pn, dp_id):
	# Query parameters as a map
	params = { "site_id": site_id, "brand": brand, "pn": pn, "dp_id": dp_id }
	# Execute coalesce query to get first non-null result
	query = con.execute('''
	SELECT COALESCE(
		(SELECT on_site FROM site_options WHERE site_id=:site_id AND brand=:brand AND pn=:pn AND dp_id=:dp_id LIMIT 1),
		(SELECT on_site FROM site_options WHERE site_id=:site_id AND brand=:brand AND pn=:pn AND dp_id=0 LIMIT 1),
		(SELECT on_site FROM site_options WHERE site_id=:site_id AND brand=:brand AND pn=\'*\' AND dp_id=0 LIMIT 1),
		(SELECT on_site FROM site_options WHERE site_id=:site_id AND brand=\'*\' AND pn=\'*\' AND dp_id=0 LIMIT 1),
		TRUE);
	''', params);
	# Fetch the result and convert to a boolean
	on_site = (query.fetchone()[0] == 1)
	return SiteOption(site_id, brand, pn, dp_id, on_site)

# Store a site option or fill into the database
def store_site_option(con, site_id, brand, pn, dp_id, on_site):
	params = { "site_id": site_id, "brand": brand, "pn": pn, "dp_id": dp_id, "on_site": on_site }
	con.execute('''
	INSERT OR REPLACE INTO site_options(site_id, brand, pn, dp_id, on_site) VALUES (:site_id, :brand, :pn, :dp_id, :on_site); 
	''', params)
	con.commit()

def main():
	# Open the SQL database connection
	con = sqlite3.connect('wf.db')
	# Make sure we have the schema set up
	create_tables(con)

	# Store a fill over all ASHLEY items using a placeholder for the PN and DP_ID columns
	store_site_option(con, 8080, "ASHLEY", "*", 0, True)
	# Store a fill over all options for a specific item using a placeholder for the DP_ID column
	store_site_option(con, 8080, "ASHLEY", "000111", 0, False)
	# Store option data for specific item
	store_site_option(con, 8080, "ASHLEY", "000111", 1000000, False)

	# Should retrieve the specific value for this option (false)
	print(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000000))
	# Should retrieve from the fill over all options for this item (false)
	print(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000001))
	# Should retrieve from the fill over all options for the ashley brand (true)
	print(fetch_site_option(con, 8080, "ASHLEY", "000113", 1000000))
	# Should retrieve the default value (true)
	print(fetch_site_option(con, 8080, "MEH", "000112", 1000000))

	# Close the database connection
	con.close()

if __name__ == '__main__':
	main()