"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 335 / 400
================================================================================
- Group: ThirdTest_pincode_group_34_parts_331_to_340
- Assigned PIN Codes: 48 (Range: 743330 to 743445)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_335.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_335.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_335"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-335] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "743330",
  "743331",
  "743332",
  "743336",
  "743337",
  "743338",
  "743345",
  "743347",
  "743348",
  "743349",
  "743351",
  "743354",
  "743355",
  "743356",
  "743357",
  "743363",
  "743368",
  "743370",
  "743371",
  "743372",
  "743373",
  "743374",
  "743375",
  "743376",
  "743377",
  "743378",
  "743383",
  "743387",
  "743395",
  "743399",
  "743401",
  "743405",
  "743411",
  "743412",
  "743422",
  "743423",
  "743424",
  "743425",
  "743426",
  "743427",
  "743428",
  "743429",
  "743435",
  "743437",
  "743438",
  "743439",
  "743442",
  "743445"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "743330": {
    "pincode": "743330",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Champahati SO",
      "Chandaneswar BO",
      "Kalikapur BO",
      "Kustia BO",
      "Noridana BO",
      "Pratapnagar BO",
      "Tarda Kapasati BO"
    ]
  },
  "743331": {
    "pincode": "743331",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Diamond Harbour HO",
      "Dakshin Hazi Pur SO",
      "Diamond Harbour CC SO",
      "Diamond Harbour New Town SO",
      "Diamond Harbour RS SO",
      "Fakir Chand College SO"
    ]
  },
  "743332": {
    "pincode": "743332",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Hatuganj SO",
      "Bardrone BO",
      "Barandari Bagaria BO",
      "Daria BO",
      "Ghatakpur BO",
      "Gumukberia BO",
      "Mahishamuri BO",
      "Manoharpur BO",
      "Narayanpur Gouripur BO",
      "Radhanagar BO",
      "Ramanandapur BO",
      "Sultanpur BO",
      "Uttar Mukundapur BO",
      "Uttar Rajarampur BO"
    ]
  },
  "743336": {
    "pincode": "743336",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Ghateswar SO",
      "Anchna BO",
      "Anwarpur BO",
      "Baidyahat BO",
      "Barakaipukur BO",
      "Bidyadharpur BO",
      "Bireswarpur BO",
      "Dhanurhat BO",
      "Dhopahat Krishnapur BO",
      "Dulalpur BO",
      "Gabberia BO",
      "Gazipur BO",
      "Jadabpurhat BO",
      "Jeliabati BO",
      "Nilambarpur BO",
      "Panchnan BO",
      "Purba Chandpur BO",
      "Ramlochanpur BO",
      "Roypur Dayarampur BO",
      "Siddheswarpur BO",
      "Uttar Ballavpur BO"
    ]
  },
  "743337": {
    "pincode": "743337",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Joynagar Majilpur SO",
      "Amtala BO",
      "Banerjee Chak BO",
      "Bantra BO",
      "Bele Durganagar BO",
      "Chaltaberia BO",
      "Charaghata BO",
      "Chatra Maldari BO",
      "Chitraganj BO",
      "Dara BO",
      "Dewanchak BO",
      "Futigoda BO",
      "Kamaria BO",
      "Khaizamara BO",
      "Kripakhali BO",
      "Maydah BO",
      "Mayahauri BO",
      "Monipur Banstala BO",
      "Rajapur Karabeg BO",
      "Sastitala BO",
      "Shyamnagar Gobindapur BO",
      "Tilpi BO",
      "Dhosa BO"
    ]
  },
  "743338": {
    "pincode": "743338",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Nimpith Ashram SO",
      "Baishata BO",
      "Bakultalahat BO",
      "Boinchbati BO",
      "Chhotrisher Hat BO",
      "Dakshin Bijoynagar BO",
      "Gopalganj Hat BO",
      "Jalaberia BO",
      "Japtala BO",
      "Kaikhali Ashram BO",
      "Keorakhali BO",
      "Kholakhali Hat BO",
      "Koabati BO",
      "Kultali BO",
      "Naskarpara Jalaberia BO",
      "Purba Ranaghata BO",
      "Taltala BO",
      "Tangrabichi BO",
      "Thakurchak BO"
    ]
  },
  "743345": {
    "pincode": "743345",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Bijoyganj Bazar SO",
      "Kashipur BO",
      "Krishnanagar BO"
    ]
  },
  "743347": {
    "pincode": "743347",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Kakdwip SO",
      "Akshoynagar BO",
      "Bamanagar BO",
      "Bhubannagar BO",
      "Bijannagar BO",
      "Bishalakshmipur BO",
      "Budhakhali BO",
      "Dakshin Gopalnagar BO",
      "Dakshin Khasiabad BO",
      "Durba Chati BO",
      "Fatickpur BO",
      "Ganeshpur 1st Gheri BO",
      "Ganeshpur 4th Gheri BO",
      "Ganeshpur Baikunthapur BO",
      "Gangadharganj BO",
      "Gobindarampur BO",
      "Harendranagar BO",
      "Jaharpur BO",
      "Kakdwip Kalinagar BO",
      "Manmathapur BO",
      "Mrinalnagar BO",
      "Muchikata Baikunthapur BO",
      "Mundapara Kashiabad BO",
      "Paschim Surendranagar BO",
      "Pukurberia BO",
      "Rajnagar BO",
      "Takipurabad BO",
      "Uttar Gopalnagar BO"
    ]
  },
  "743348": {
    "pincode": "743348",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Karanjali SO",
      "Belpukur BO",
      "Dakshin Rajarampur BO",
      "Jamtala BO",
      "Keoratala BO",
      "Seoratala BO",
      "Tengrarchar BO"
    ]
  },
  "743349": {
    "pincode": "743349",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Kashinagar SO South 24 Parganas",
      "Baburchak BO",
      "Bakultala BO",
      "Barashi BO",
      "Baribhanga Abad BO",
      "Biswanathpur BO",
      "Boyergodi BO",
      "Chapla BO",
      "Chuprijhara BO",
      "Dakshin Durgapur BO",
      "Dakshin Raipur No 3 BO",
      "Dakshin Shibpur BO",
      "Deulbari BO",
      "Digambarpur BO",
      "Gheri Keorakhali BO",
      "Gheri Raipur BO",
      "Gurudaspur BO",
      "Kailaspur BO",
      "Katamari BO",
      "Khari BO",
      "Kumarpur BO",
      "Madhabpur BO",
      "Madhusudanchak BO",
      "Madhya Gurguria BO",
      "Madhya Monirtat BO",
      "Manirtat BO",
      "Meherpur BO",
      "Nandakumarpur BO",
      "Pakurtala BO",
      "Paschim Radhaballavpur BO",
      "Purandarpur BO",
      "Purba Sonatikari BO",
      "Purnachandrapur BO",
      "Radhaballavpur BO",
      "Ramnagarabad BO",
      "Saratpally BO",
      "Sonatikary BO",
      "Sridurgapur BO",
      "Kautala BO",
      "Bhubankhali BO"
    ]
  },
  "743351": {
    "pincode": "743351",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Kulpi SO",
      "Enayetpur BO",
      "Paschim Gopalnagar BO",
      "Ramkrishnapur BO",
      "Shyambasurchak BO",
      "Sitarampur BO"
    ]
  },
  "743354": {
    "pincode": "743354",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Mathurapur RS SO",
      "Bairagir Chak BO",
      "Balarampur BO",
      "Batiswar BO",
      "Dahakanda BO",
      "Ghoradah BO",
      "Ghoserpole BO",
      "Gilarchat BO",
      "Kayalerchak BO",
      "Khojkhidir BO",
      "Krishnachandrapur BO",
      "Lalpur BO",
      "Nalua BO",
      "Nayabad Rajpur BO",
      "Panapukur BO",
      "Paschim Kultali BO",
      "Purkaitpara BO",
      "Radhakantapur BO",
      "Saratnagar BO",
      "Tajpur BO",
      "Uttar Lakshminarayanpur BO",
      "Mathura Pur SO"
    ]
  },
  "743355": {
    "pincode": "743355",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Magrahat SO",
      "Alida BO",
      "Bamna BO",
      "Benipur BO",
      "Dhanpota BO",
      "Dhanyaghata BO",
      "Haripur BO",
      "Jugdia BO",
      "Kadipukur BO",
      "Kalikapota BO",
      "Khonkarbazar BO",
      "Lakshmikantapur BO",
      "Magrahat Shyampur BO",
      "Maheshpur BO",
      "Maitala BO",
      "Maitirhat BO",
      "Mallick Pratap BO",
      "Morapai BO",
      "Purba Ramnathpur BO",
      "Sangrampur Hat BO",
      "Tekpanja BO",
      "Urel Chandpur BO",
      "Gokarnee BO",
      "Magrahat Shyamnagar BO"
    ]
  },
  "743356": {
    "pincode": "743356",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Mud Point SO",
      "Ghoramara Mandirtala BO"
    ]
  },
  "743357": {
    "pincode": "743357",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Nam Khana SO",
      "Bagdanga BO",
      "Baliara BO",
      "Chandanpiri BO",
      "Dakshin Chandannagar BO",
      "Debnagar BO",
      "Debnibas BO",
      "Durgapur Tekarbazar BO",
      "Dwariknagar BO",
      "Ganeshnagar BO",
      "Haripur BO",
      "Kusumtala BO",
      "Lakshmipur Pabartak BO",
      "Madanganj BO",
      "Maharajganj BO",
      "Namkhana Radhanagar BO",
      "Patibunia BO",
      "Rajnagar Biswambhar BO",
      "Shibrampur BO",
      "Shibnagar Abad BO",
      "Freser Ganj BO"
    ]
  },
  "743363": {
    "pincode": "743363",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Bansra SO",
      "Gourdaha BO",
      "Kalaria BO",
      "Narayanpur BO"
    ]
  },
  "743368": {
    "pincode": "743368",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Sarisha SO",
      "Abdalpur BO",
      "Amira BO",
      "Basuldanga BO",
      "Boria BO",
      "Chandnagar BO",
      "Darikrishna Nagar BO",
      "Goanara Gobindapur BO",
      "Kamarpole BO",
      "Kholakhali BO",
      "Kodalia Shibpur BO",
      "Maheswara BO",
      "Mathur BO",
      "Morigachi BO",
      "Mukundapur BO",
      "Nawasa BO",
      "Netra BO",
      "Noorpur BO",
      "Panchagram Singhberia BO",
      "Patra BO"
    ]
  },
  "743370": {
    "pincode": "743370",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Gosaba SO",
      "Amlamethi BO",
      "Arampur BO",
      "Bali Hatkhola BO",
      "Bijoynagar BO",
      "Chargheri BO",
      "Dayapur BO",
      "Dulki BO",
      "Jamespur BO",
      "Harishpur Ramnagar BO",
      "Jahar Colony BO",
      "Kachukhali  Ramnagar Bazar BO",
      "Kachukhali BO",
      "Lahiri Pur BO",
      "Luxbagan BO",
      "Manmathanagar M BO",
      "Pakhiralaya BO",
      "Parash Moni BO",
      "Rangabelia BO",
      "Sadhu Pur BO",
      "Santi Gachi BO",
      "Satjelia BO",
      "Satyanarayanpur BO",
      "Uttardanga BO"
    ]
  },
  "743371": {
    "pincode": "743371",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Pathar Pratima SO",
      "Banashyamnagar BO",
      "Baradapur BO",
      "Bhagabatpur Project",
      "Brajaballav Pur BO",
      "Buraburirtat BO",
      "Choto Banashyamnagar BO",
      "Daspur BO",
      "Gobindapur Abad BO",
      "Indrapur BO",
      "Jogindrapur BO",
      "Kamdebpur BO",
      "Khetramohan Pur BO",
      "Krishnadaspur BO",
      "Madhabnagar Durgagobindapur BO",
      "Rakhal Pur BO",
      "Rakshashkhali BO",
      "Ramganga BO",
      "Shibgobindapur BO",
      "Sridharnagar BO",
      "Uttarsitaram Pur BO"
    ]
  },
  "743372": {
    "pincode": "743372",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Dakshin Barasat SO",
      "Baharu BO",
      "Banasundaria BO",
      "Gorerhat BO",
      "Kashimpur BO",
      "Keyatala BO",
      "Khakurdaha BO",
      "Madhya Shibpur BO",
      "Nabagram BO",
      "Punpua BO",
      "Srikrishnanagar BO",
      "Surajapurhat BO",
      "Tasarala BO",
      "Gocharan BO",
      "Sarberia BO"
    ]
  },
  "743373": {
    "pincode": "743373",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Rudra Nagar SO",
      "Bamankhali BO",
      "Beguakhali BO",
      "Chakfuldubi BO",
      "Chemaguri BO",
      "Companir Char BO",
      "Dakshin Haradhanpur BO",
      "Dhablat Shibpur BO",
      "Dihimathurapur Gobindapur BO",
      "Haradhan Pur BO",
      "Harinbari BO",
      "Jibantala BO",
      "Kachuberia BO",
      "Kalitala Daspur BO",
      "Kamalpur BO",
      "Khasramkarchak BO",
      "Khasmahal BO",
      "Khansahebabad BO",
      "Kirtankhali BO",
      "Lalpur BO",
      "Mahendraganj BO",
      "Mandirtala BO",
      "Mrityunjoynagar BO",
      "Nagendraganj BO",
      "Narayanibad BO",
      "Natendrapur BO",
      "Prasad Pur BO",
      "Radhakrishnapur BO",
      "Sagar Bag Bazar BO",
      "Sagar Barabari BO",
      "Sagar Chandipur BO",
      "Sagar Fulbari BO",
      "Sagar Krishna Nagar BO",
      "Sagar Bishnupur BO",
      "Sagar Shibpur BO",
      "Sapkhali BO",
      "Sumatinagar BO",
      "Mansadwip BO",
      "Muri Ganga BO",
      "Gangasagar BO"
    ]
  },
  "743374": {
    "pincode": "743374",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Nishchinta Pur SO",
      "Aswathtala BO",
      "Basar BO",
      "Dakshin Chandipur BO",
      "Dakshin Rajnagar BO",
      "Gazirmahal Chitamari BO",
      "Madhabnagar BO",
      "Manicknagar BO",
      "Raghunathpur BO",
      "Ramnagar Shibpur BO",
      "Ramtanunagar BO",
      "Ramratanpur BO",
      "Shibkalinagar BO",
      "Srinagar Makarpara BO",
      "Surjanagar BO",
      "Tepakhali BO"
    ]
  },
  "743375": {
    "pincode": "743375",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Usthi SO",
      "Baneswarpur BO",
      "Borijpur BO",
      "Boinchberia BO",
      "Kensilly BO",
      "Kuleswar BO",
      "Masat BO",
      "Nazra BO",
      "Rasa Dogacha BO",
      "Sarachi BO",
      "Uttar Kusum BO"
    ]
  },
  "743376": {
    "pincode": "743376",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Taldi SO",
      "Belegachi BO",
      "Bhalleya BO",
      "Chandkhali BO",
      "Chhayani BO",
      "Debisabad BO",
      "Dhalirbati BO",
      "Doltalaghola BO",
      "Jibantala Sarangabad BO",
      "Khaskumrakhali BO",
      "Nagartala BO",
      "Thakuraniberia BO"
    ]
  },
  "743377": {
    "pincode": "743377",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Bakrahat SO",
      "Aryapara BO",
      "Chowl Khola BO",
      "Muchisa BO",
      "Patharberia BO",
      "Sonaria Naiya BO"
    ]
  },
  "743378": {
    "pincode": "743378",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Chhoto Mollakhali SO",
      "Amtali BO",
      "Amtali Bazar BO",
      "Bara Mollakhali BO",
      "Chimta BO",
      "Dakshin Radhanagar BO",
      "Kalidaspur BO",
      "Kumirmari BO",
      "Kumirmari Narendrapur BO",
      "Nathpara BO",
      "Paschim Radhanagar BO",
      "Puinjali BO",
      "Purba Radhanagar BO",
      "Taranagar BO"
    ]
  },
  "743383": {
    "pincode": "743383",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Raidighi SO",
      "Achintanagar BO",
      "Baikunthapur BO",
      "Bhubaneswari Bazar BO",
      "Binodpur Ambikanagar BO",
      "Dakshin Kashinagar BO",
      "Damkal BO",
      "Debipur BO",
      "Goalara BO",
      "Heramba Gopalpur BO",
      "Jata BO",
      "Jata Jogendrapur BO",
      "Kankandighi BO",
      "Kedarpur BO",
      "Kishorimohanpur BO",
      "Kuemuri BO",
      "Kumrapara BO",
      "Lakshmi Janardanpur BO",
      "Nagenabad BO",
      "Nagendrapur BO",
      "Narayanipur BO",
      "Paschim Jata BO",
      "Paschim Sripatinagar BO",
      "Purba Dwarikapur BO",
      "Purba Sridharpur BO",
      "Uttar Kankandighi BO"
    ]
  },
  "743387": {
    "pincode": "743387",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Piyali Town SO",
      "Begampur BO",
      "Brindakhali BO",
      "Dhapdhapi BO",
      "Kumarhat BO",
      "Ramgopalpur BO",
      "Sikharbali BO",
      "Sitakundu BO",
      "South Ramnagar BO",
      "Baruipur Bazar BO"
    ]
  },
  "743395": {
    "pincode": "743395",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "South Bishnupur SO",
      "Bazarberia BO",
      "Chaitanyapur BO",
      "Chandpur BO",
      "Digberia BO",
      "Kecharkur Gopalnagar BO",
      "Mandir Bazar BO",
      "Pranballavpur BO",
      "Purba Gopalnagar BO",
      "Ramakantanagar BO",
      "Satghara Debipur BO",
      "Suta Becha BO"
    ]
  },
  "743399": {
    "pincode": "743399",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "South Presidency Division",
    "offices": [
      "Dholahat SO",
      "Bahirchak BO",
      "Bhagabatipur BO",
      "Bhajna Benepara BO",
      "Bhatia BO",
      "Bhagabanpur BO",
      "Bohalberia BO",
      "Chandipur BO",
      "Dakshin Gangadharpur BO",
      "Dakshin Jagadishpur BO",
      "Durganagar BO",
      "Harindanga BO",
      "Harinkhola BO",
      "Imamuddipur BO",
      "Jumainaskar BO",
      "Konchfal BO",
      "Malaya BO",
      "Raghudebpur BO",
      "Ramchandranagar BO",
      "Ramchandrapur BO",
      "Ramdebpur BO",
      "Shibnagar BO",
      "Siddhiberia BO",
      "Srinarayanpur BO",
      "Taranagar BO",
      "Uttar Birampur BO"
    ]
  },
  "743401": {
    "pincode": "743401",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Baduria SO",
      "Andharmanik BO",
      "Bena BO",
      "Buruj BO",
      "Jasaikati BO",
      "Nayabastia BO",
      "Raghunathpur BO",
      "Rudrapur BO",
      "Simulia Durgapur BO",
      "Taragunia BO"
    ]
  },
  "743405": {
    "pincode": "743405",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Petrapole SO",
      "Chhoygharia BO",
      "Khalitpur BO"
    ]
  },
  "743411": {
    "pincode": "743411",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Basirhat HO",
      "Basirhat Bazar SO"
    ]
  },
  "743412": {
    "pincode": "743412",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Basirhat College SO",
      "Basirhat RS BO",
      "Dandirhat BO",
      "Dhaltitha BO",
      "Harishpur BO",
      "Karnalkora BO"
    ]
  },
  "743422": {
    "pincode": "743422",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Bhabla SO",
      "Biramnagar BO",
      "Gotra BO",
      "Kathur BO",
      "Makhalgachha BO",
      "Paikparahat BO",
      "Pifa BO",
      "Sangrampur BO",
      "Jirakpur BO"
    ]
  },
  "743423": {
    "pincode": "743423",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Deganga SO",
      "Baidyapur BO",
      "Beliaghata Bridge BO",
      "Belpur BO",
      "Bhasila BO",
      "Gobardhanpur BO",
      "Golabari Bazar BO",
      "Kaliani BO",
      "Karua Chandpur BO",
      "Majlishpur BO",
      "Parulia BO",
      "Fazilpur BO",
      "Ramnathpur BO",
      "Sandalia BO",
      "Sasan BO",
      "Sohai Kumarpur BO"
    ]
  },
  "743424": {
    "pincode": "743424",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Debalaya SO",
      "Chakla BO",
      "Chandpur BO",
      "Chowrashi BO",
      "Gobila BO",
      "Hadipur BO",
      "Kachua BO",
      "Secondranagar BO"
    ]
  },
  "743425": {
    "pincode": "743425",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Haroa SO",
      "Minakhan BO",
      "Adampur BO",
      "Aziznagar BO",
      "Atpukur BO",
      "Bamanpukur BO",
      "Chowhata BO",
      "Dhuturdah BO",
      "Joygram BO",
      "Khalisadi BO",
      "Kharupala BO",
      "Khasbalanda BO",
      "Kumarjole BO",
      "Malancha BO",
      "Narabunia BO",
      "Raikhan BO",
      "Uchildah BO"
    ]
  },
  "743426": {
    "pincode": "743426",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Hasnabad SO",
      "Barunhat BO",
      "Bhurkunda BO",
      "Chakpatli BO",
      "Ghuni BO",
      "Ghosalbati BO",
      "Ichhapur Abad BO",
      "Rameswarpur BO",
      "Simulia Kalibari BO",
      "Sulkuniabad BO"
    ]
  },
  "743427": {
    "pincode": "743427",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Katiahat SO",
      "Aturia BO",
      "Bajitpur BO",
      "Barabankra BO",
      "Bhaduria BO",
      "Bhojpara BO",
      "Gandharbapur BO",
      "Goaldah BO",
      "Kaijuri BO",
      "Khorgachi BO",
      "Model Belghoria BO",
      "Paschim Palta BO",
      "Punra BO",
      "Safirabad SC Ghosh Rd BO",
      "Sayestanagar BO"
    ]
  },
  "743428": {
    "pincode": "743428",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Kholapota SO",
      "Raghunathpur BO",
      "Sikrakulingram BO",
      "Srinagar BO",
      "Zafarpur BO"
    ]
  },
  "743429": {
    "pincode": "743429",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Taki SO",
      "Dakshin Bagundi BO",
      "Kishor Nagar Jalalpur BO",
      "Paschim Madhyampur BO",
      "Soladana BO",
      "Takipur BO",
      "Taki Market SO"
    ]
  },
  "743435": {
    "pincode": "743435",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Hingalganj SO",
      "Amberia BO",
      "Bankra Dabar BO",
      "Bankra BO",
      "Banshtala BO",
      "Bishpur BO",
      "Dharamberia BO",
      "Kanchanpur BO",
      "Kothabari BO",
      "Purba  Khejurberia BO",
      "Rupmari BO",
      "Sandeler Bil BO",
      "Takimamudpur BO"
    ]
  },
  "743437": {
    "pincode": "743437",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Dhanyakuria SO",
      "Arbelia BO",
      "Begampur BO",
      "Matia BO",
      "Nehalpur BO"
    ]
  },
  "743438": {
    "pincode": "743438",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Kolsur SO",
      "Atghara BO",
      "Jangalpur BO",
      "Kamdevkati BO",
      "Mandra BO",
      "Parpatna BO"
    ]
  },
  "743439": {
    "pincode": "743439",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Hatgachha SO",
      "Bhanderkhali BO",
      "Chandanpur BO",
      "Choto Sahebkhali BO",
      "Dakshin Hatgachha BO",
      "Deuli Pukuria BO",
      "Dulduli Mathbari BO",
      "Hasnabad Rampur BO",
      "Hemnagar BO",
      "Jogeshganj BO",
      "Kalitala BO",
      "Lebukhali BO",
      "Madhabkati BO",
      "Malekangumti BO",
      "Kanaikati BO",
      "Parghumti BO",
      "Paschim Parghumti BO",
      "Paschim Sridharkati BO",
      "Patghara BO",
      "Purba Deuli BO",
      "Sahebkhali BO",
      "Samsernagar BO",
      "South Charalkhali BO",
      "Sridharkati BO",
      "Uttar Malikanghumti BO"
    ]
  },
  "743442": {
    "pincode": "743442",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Nazat Hatkhola SO",
      "Baunia Abad BO",
      "Bermajur BO",
      "Boyermari BO",
      "Choto Sehara BO",
      "Dakshin Akhratala BO",
      "Dheknamari BO",
      "Ghotihara BO",
      "Ghoshpara BO",
      "Jhupkhali BO",
      "Kalinagar Hat BO",
      "Kanmari BO",
      "Khariahatabad BO",
      "Mathbari Abad BO",
      "Nalkora BO",
      "Nityaberia BO",
      "Purba Sankardaha BO",
      "South Sikarigheri BO"
    ]
  },
  "743445": {
    "pincode": "743445",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Barasat Division",
    "offices": [
      "Gopalpur SO North 24 Parganas",
      "Amta BO",
      "Bhabanipur BO",
      "Chaita BO",
      "Ghona BO",
      "Gouribhoj BO",
      "Sadarpur Behala BO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
