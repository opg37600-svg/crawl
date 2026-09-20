"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 332 / 400
================================================================================
- Group: ThirdTest_pincode_group_34_parts_331_to_340
- Assigned PIN Codes: 48 (Range: 741246 to 742147)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_332.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_332.csv & .json
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

PART_ID = "part_332"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-332] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "741246",
  "741247",
  "741248",
  "741249",
  "741250",
  "741251",
  "741252",
  "741253",
  "741254",
  "741255",
  "741256",
  "741257",
  "741301",
  "741302",
  "741313",
  "741315",
  "741316",
  "741317",
  "741319",
  "741401",
  "741402",
  "741404",
  "741501",
  "741502",
  "741503",
  "741504",
  "741505",
  "741506",
  "741507",
  "741508",
  "741509",
  "742101",
  "742102",
  "742103",
  "742104",
  "742113",
  "742121",
  "742122",
  "742123",
  "742132",
  "742133",
  "742134",
  "742135",
  "742136",
  "742137",
  "742138",
  "742140",
  "742147"
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
  "741246": {
    "pincode": "741246",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Mohanpur SO Nadia",
      "Haringhat River Research Insti BO"
    ]
  },
  "741247": {
    "pincode": "741247",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Pritinagar SO"
    ]
  },
  "741248": {
    "pincode": "741248",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Simurali SO",
      "Bhagirathisilpaashram BO",
      "Chanduria BO",
      "Narapatipara BO",
      "Rautari BO"
    ]
  },
  "741249": {
    "pincode": "741249",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Subarnapur SO",
      "Fatepur BO",
      "Mollabelia BO",
      "Narayanpur BO",
      "Panpur BO",
      "Simhat BO"
    ]
  },
  "741250": {
    "pincode": "741250",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Kataganj SO",
      "Bedibhavan BO"
    ]
  },
  "741251": {
    "pincode": "741251",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Netajisubhas Sanatorium SO",
      "Charsarhati BO",
      "Majerchar BO"
    ]
  },
  "741252": {
    "pincode": "741252",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Krishivisyavidyalaya SO"
    ]
  },
  "741253": {
    "pincode": "741253",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Anishmali SO",
      "Eruli BO"
    ]
  },
  "741254": {
    "pincode": "741254",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Kalinarayanpur SO",
      "Kamgachi BO"
    ]
  },
  "741255": {
    "pincode": "741255",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Anulia SO",
      "Haradham BO",
      "Nandighat BO",
      "Noapara BO"
    ]
  },
  "741256": {
    "pincode": "741256",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Begopara SO",
      "Dayabari BO",
      "Ranaghat Baidyapur BO"
    ]
  },
  "741257": {
    "pincode": "741257",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Nagarukhra SO",
      "Dighalgram BO",
      "Haripukuria BO",
      "Kasthadanga BO",
      "Mahadevpur BO",
      "Nimtala Bazar BO"
    ]
  },
  "741301": {
    "pincode": "741301",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Char Brahmanagar SO",
      "Ushidpur BO"
    ]
  },
  "741302": {
    "pincode": "741302",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Nabadwip HO",
      "Buroshibtala SO Nadia",
      "Dearapara SO",
      "Kolerdanga SO",
      "Nabadwip Bazar SO",
      "Nabadwip Dham RS SO",
      "Poramatala Road SO",
      "Pratapnagar SO Nadia",
      "Tegharipara SO",
      "Motiroybnadh BO",
      "Rudrapara BO"
    ]
  },
  "741313": {
    "pincode": "741313",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Sreemayapur SO",
      "Bamunpukur BO",
      "Ganja Danga BO",
      "Nidaya BO"
    ]
  },
  "741315": {
    "pincode": "741315",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Swarupganj SO",
      "Amghata BO",
      "Gadigachha BO",
      "Mahesganj BO"
    ]
  },
  "741316": {
    "pincode": "741316",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Srirampur SO Nadia",
      "Bablari BO",
      "Bahirchara BO",
      "Fakirdanga BO",
      "Idrakpur BO",
      "Mahisura BO"
    ]
  },
  "741317": {
    "pincode": "741317",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Joania Bhaluka SO",
      "Aluipara BO",
      "Joania BO"
    ]
  },
  "741319": {
    "pincode": "741319",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia North Division",
    "offices": [
      "Bidyanagar SO"
    ]
  },
  "741401": {
    "pincode": "741401",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Dignagar SO"
    ]
  },
  "741402": {
    "pincode": "741402",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Fulia Colony SO",
      "2noNatunfulia BO",
      "Belemath BO",
      "Buincha BO",
      "Fulia Boyra BO",
      "Gazipur BO",
      "Habibpur BO",
      "Panpara BO",
      "Tarapur BO"
    ]
  },
  "741404": {
    "pincode": "741404",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Santipur SO",
      "Bablagobindapur BO",
      "Bagachra BO",
      "Barajiakur BO",
      "Gayeshpur Hazratala BO",
      "Ghoralia BO",
      "Haripurnadia BO",
      "Khandakhola BO",
      "Nrisinghapur BO",
      "Ashanandapara Street SO",
      "Krishnakalitala SO",
      "Santipur RS SO",
      "Shyamchandpara SO",
      "Sutragarh SO",
      "Ramnagarchar BO",
      "Santipur College BO",
      "Barabazar BO",
      "Kuthirpara BO"
    ]
  },
  "741501": {
    "pincode": "741501",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Arangahta SO",
      "Barbaria BO",
      "Churni Raghunathpur BO",
      "Hatbahirgachi BO",
      "Panchberia BO",
      "Purbanowpara BO",
      "Shibnarayanpur BO"
    ]
  },
  "741502": {
    "pincode": "741502",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Bagula SO",
      "Benali BO",
      "Bhairabchandrapur BO",
      "Bhayna BO",
      "Chupria BO",
      "Garapotanadia BO",
      "Haldipara BO",
      "Haritala BO",
      "Kumari Ramnagar BO",
      "Mayurhat BO",
      "Muraghachacolony BO",
      "Nabarupdaha BO",
      "Patikabari BO",
      "Santinagar BO",
      "Taraknagar BO",
      "Ulashi BO"
    ]
  },
  "741503": {
    "pincode": "741503",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Banpur SO Nadia",
      "Bijoypur BO",
      "Gede BO",
      "Raghu Nandan Pur BO"
    ]
  },
  "741504": {
    "pincode": "741504",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Duttapulia SO",
      "Baranberia BO",
      "Daluabari BO"
    ]
  },
  "741505": {
    "pincode": "741505",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Hanskhali SO",
      "Betna BO",
      "Daharbuincha BO",
      "Gobindapur BO"
    ]
  },
  "741506": {
    "pincode": "741506",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Krishnaganj SO",
      "Durgapur BO",
      "Swarnakhali BO",
      "Tangra BO"
    ]
  },
  "741507": {
    "pincode": "741507",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Majdia SO",
      "Bathangachi BO",
      "Bhajanghat BO",
      "Fatepur Majdia BO",
      "Gazna BO",
      "Mathurapur BO",
      "Putikhali BO",
      "Taldaha BO"
    ]
  },
  "741508": {
    "pincode": "741508",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Khalboalia SO",
      "Digambarpur BO",
      "Joyghata BO",
      "Maheshpur BO",
      "Nadiasumalia BO"
    ]
  },
  "741509": {
    "pincode": "741509",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Nadia South Division",
    "offices": [
      "Chandannagar SO Nadia",
      "Chowgacha BO",
      "Shibnibas BO"
    ]
  },
  "742101": {
    "pincode": "742101",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Berhampore WB HO",
      "Berhampore Civil Court SO",
      "Berhampore Station Road SO",
      "District School Board SO Murshidabad",
      "Gorabazar SO",
      "Kansari Bazar SO",
      "Laldighi SO",
      "Madhupur Colony SO",
      "SSSen Road SO"
    ]
  },
  "742102": {
    "pincode": "742102",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Cossimbazar Raj SO",
      "Banjetia BO",
      "Boaliadanga BO",
      "Gazdharpara BO",
      "Hatinagar BO",
      "Muktinagar BO",
      "Nograjole BO",
      "Nowdapanur BO",
      "Sundipur BO",
      "Chunakhali Nimtala SO",
      "Bishnupur Kalibari BO"
    ]
  },
  "742103": {
    "pincode": "742103",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Khagra SO",
      "Daihatta Road SO",
      "Indraprastha SO",
      "Kunjaghata BO",
      "Saidabad BO"
    ]
  },
  "742104": {
    "pincode": "742104",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "DLSSJdham SO",
      "Chotobathan BO",
      "Etore BO",
      "Kiriteswari BO"
    ]
  },
  "742113": {
    "pincode": "742113",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Debipur SO",
      "Dalelpara BO",
      "Rampur BO"
    ]
  },
  "742121": {
    "pincode": "742121",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Amtala SO",
      "Bhalanagar BO",
      "Chandpur BO",
      "Gangadhari BO",
      "Garibpur BO",
      "Goghata BO",
      "Jagaipur BO",
      "Jitpur BO",
      "Kanainagar BO",
      "Madhupur BO",
      "Mohammadpur BO",
      "Nowda BO",
      "Pardiar BO",
      "Patikabari BO",
      "Raipurmirpur BO"
    ]
  },
  "742122": {
    "pincode": "742122",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Azimganj SO",
      "Adibasi Beldanga BO",
      "Dasturhat BO",
      "Diara BO",
      "Ghorshala BO",
      "Ghugridanga BO",
      "Gobardhandanga BO",
      "Khatua BO",
      "Nagra BO",
      "Patkeldanga BO",
      "Sahapurbarala BO",
      "Singheswari Gouripur BO"
    ]
  },
  "742123": {
    "pincode": "742123",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Jiaganj SO",
      "AMAIPARA BO",
      "AMDAHARA BO",
      "Bhandara BO",
      "KOLAN RADHAKANTAPUR BO",
      "SADEKBAG BO",
      "SRIPAT SINGH COLLEGE BO"
    ]
  },
  "742132": {
    "pincode": "742132",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Barwan SO Murshidabad",
      "Ekghoria Khanqua BO",
      "Ekpaharia Sayedpur BO",
      "Fatepur BO",
      "Gramshalika BO",
      "Jugsara BO",
      "Kantur BO",
      "Kurulia BO",
      "Mahura Kandi BO",
      "Satitara BO",
      "Senai BO"
    ]
  },
  "742133": {
    "pincode": "742133",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Mahishmara BO",
      "Maniknagar BO",
      "Mirzapur SO MurshidabBO",
      "Beldanga SO",
      "Naupukuria BO",
      "Begunbari BO",
      "Bishannagar BO",
      "Dalua BO",
      "Debkunda BO",
      "Ghoramara BO",
      "Hareknagar BO",
      "Kalitala BO",
      "Kapasdanga BO",
      "Kazisaha BO",
      "Kedarchandpur BO",
      "Madda BO"
    ]
  },
  "742134": {
    "pincode": "742134",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Bezpara Saluadanga BO",
      "Bhabta SO",
      "Bhadur BO",
      "Chaitanyapur BO",
      "Debipur BO",
      "Gopinathpur BO",
      "Jhunka BO",
      "Khidirpur BO",
      "Mahula BO",
      "Nowpara Simulia BO",
      "Pulinda BO",
      "Rameswarpur BO",
      "Sujapur BO"
    ]
  },
  "742135": {
    "pincode": "742135",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Bhagabangola SO",
      "Akheriganj BO",
      "Bahadurpur BO",
      "Bhagabangola Sundarpur BO",
      "Debaipur BO",
      "Diar Fatepur BO",
      "Diar Shyampur BO",
      "Fulpur BO",
      "Habaspur BO",
      "Hanumanta Nagar BO",
      "Jajriachar Dumuria BO",
      "Jibanpur BO",
      "Kalukhali BO",
      "Kantanagar BO",
      "Khamer Diar BO",
      "Mahisasthali BO",
      "Nashipur Balagachi BO",
      "Nayakharida Babupur BO",
      "Shyampur Nutanpara BO"
    ]
  },
  "742136": {
    "pincode": "742136",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Gokarna SO",
      "Amritakunda BO",
      "Bagore BO",
      "Ganthla BO",
      "Gobor Hati BO",
      "Hazarpur Nabagram BO",
      "Jibanti BO",
      "Khosbaspur BO",
      "Kumar Shanda BO",
      "Mahalandi BO",
      "Mahurul Anantapur BO"
    ]
  },
  "742137": {
    "pincode": "742137",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Kandi HO",
      "Kandi Bazar SO",
      "Kandi Bus Stand SO",
      "Kandi Court SO"
    ]
  },
  "742138": {
    "pincode": "742138",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Bagdanga SO",
      "Andulia BO",
      "Bahara BO",
      "Brahmanpara BO",
      "Chhatina Kandi BO",
      "Kandi Balia BO",
      "Mahadia BO",
      "Rasorah BO",
      "Jemo BO"
    ]
  },
  "742140": {
    "pincode": "742140",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Jemo Rajbati SO",
      "Benipur BO",
      "Choatore BO",
      "Gangedda BO",
      "Gunanandabati BO",
      "Jajan BO",
      "Kolla BO",
      "Nutangram BO",
      "Purandarpur BO",
      "Rajarampur Monoharpur BO"
    ]
  },
  "742147": {
    "pincode": "742147",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Murshidabad Division",
    "offices": [
      "Khargram SO",
      "Ayra BO",
      "Badua BO",
      "Eroali BO",
      "Gayespur BO",
      "Gurulia BO",
      "Haripur BO",
      "Jasohari BO",
      "Khargram Hospital BO",
      "Kirtipur BO",
      "Mahisar BO",
      "Margram BO"
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
