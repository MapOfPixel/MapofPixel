name = "pixel-dunya"
main = "worker.js"
compatibility_date = "2025-09-01"

[[durable_objects.bindings]]
name = "WORLD"
class_name = "World"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["World"]
