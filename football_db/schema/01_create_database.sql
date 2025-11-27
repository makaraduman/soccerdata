-- ============================================================
-- Football Statistics Database - Main Database Creation
-- ============================================================
-- This script creates the main database for football statistics
-- Run this script first before running other schema scripts

-- Create database (run as superuser)
CREATE DATABASE football_stats
    WITH
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE = template0;

-- Connect to the database
\c football_stats;

-- Create schema for organizing tables
CREATE SCHEMA IF NOT EXISTS football;

-- Set default schema
SET search_path TO football, public;

-- Create extension for UUID support
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create extension for trigram similarity (useful for team name matching)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

COMMENT ON DATABASE football_stats IS 'Database for storing historical football statistics from top European leagues';
COMMENT ON SCHEMA football IS 'Main schema containing all football statistics tables';
