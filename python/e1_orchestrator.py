from modules import *


# Main function to orchestrate the database setup

def main():
    
    # Set up logging
    
    setup_logging("logs/exercise_one.log")

    # Set up SQLite database connection
    
    conn, cursor = setup_sqlite()
    
    try:

        # Create tables and insert data
        
        create_tables(cursor, "e1")
        
        insert_data(cursor, "e1")
                
        # Fetch and format results
        
        results = "Exercise 1)\n"
        
        results += fetch_and_format_results(cursor, "e1_q1", "Question 1", "Quais são os 50 maiores produtores em faturamento ($) de 2021?")
        
        results += fetch_and_format_results(cursor, "e1_q2", "Question 2", "Quais são os 2 produtos que mais faturaram ($) de cada produtor?")
        
        # Write results to answers.txt
        
        with open("e1_answers.txt", "w") as f:
            
            f.write(results)
        
        logging.info("Database setup and query execution completed successfully.")
        
    except Exception as e:
        
        logging.error(f"An error occurred: {e}")
        
    finally:
        
        # Close the connection
        
        conn.close()
        
        logging.info("Database connection closed.")

if __name__ == "__main__":
    
    main()
