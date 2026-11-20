-- the owner runs the journal's migrations and owns the database; the app serves requests
CREATE USER accounts_owner WITH PASSWORD 'accounts_owner_pass';
CREATE USER accounts_app WITH PASSWORD 'accounts_app_pass';

CREATE DATABASE accounts OWNER accounts_owner;
