from dataclasses import dataclass
from random import Random


@dataclass(frozen=True)
class Product:
    product_id: str
    category_id: str
    base_price: int


class Catalog:
    def __init__(self) -> None:
        self.categories = [
            "c_electronics",
            "c_fashion",
            "c_beauty",
            "c_grocery",
            "c_sports",
            "c_books",
        ]
        self.products = [
            Product(f"p_{index:05d}", category_id, 5000 + index * 750)
            for index, category_id in enumerate(self.categories * 30, start=1)
        ]

    def random_product(self, random: Random) -> Product:
        return random.choice(self.products)

    def random_product_in_category(self, random: Random, category_id: str) -> Product:
        products = [product for product in self.products if product.category_id == category_id]
        if not products:
            products = self.products
        return random.choice(products)

