# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)


        # Remove leading and trailing whitespaces
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)

                adapter[field_name] = value[0].strip()

        # Category and Product type switch to lower case
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()


        # price converting to float
        price_keys = ['price','price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£','')
            adapter[price_key] = float(value)

        # change availabity to number only

        availabilty_string = adapter.get('availability')
        split_string_array = availabilty_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availabilty_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availabilty_array[0])


        # reviews => convert string to number
        num_reviews_string  = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        # star rating
        star_string = adapter.get('stars')
        split_stars_array = star_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == 'zero':
            adapter['stars'] = 0
        elif stars_text_value == 'one':
            adapter['stars'] = 1
        elif stars_text_value == 'two':
            adapter['stars'] = 2
        elif stars_text_value == 'three':
            adapter['stars'] = 3
        elif stars_text_value == 'four':
            adapter['stars'] = 4
        elif stars_text_value == 'five':
            adapter['stars'] = 5

        return item

import mysql.connector
class SaveToMySqlPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1324",
            database="books"
        )

        ## create cursor, used to execute queries
        self.cur = self.conn.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id int not null auto_increment,
            url varchar(255),
            title text,
            upc varchar(255),
            product_type varchar(255),
            price_excl_tax decimal,
            price_incl_tax decimal,
            tax decimal,
            price decimal,                        
            availability integer,
            num_reviews integer,      
            stars integer,
            category varchar(255),
            description text,
            primary key(id)                                     
        )
        """)


    def process_item(self, item, spider):
        self.cur.execute(""" insert into books (
            url,
            title,
            upc,
            product_type,
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            stars,
            category,
            description
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
            item['url'],
            item['title'],
            item['upc'],
            item['product_type'],
            item['price_excl_tax'],
            item['price_incl_tax'],
            item['tax'],
            item['price'],
            item['availability'],
            item['num_reviews'],
            item['stars'],
            item['category'],
            str(item['description'][0]),
            )
        )

        self.conn.commit()
        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()