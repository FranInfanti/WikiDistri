import ast

from datetime import datetime
from dataclasses import dataclass

_FIELDS = 7

@dataclass
class WikipediaArticle:
    abstract: str
    name: str
    url: str
    date_modified: datetime | None
    image: str | None
    in_language: str | None
    sections: str

    @classmethod
    def from_csv(cls, csv_line: str):
        fields = csv_line.strip().split(",")
        if len(fields) != _FIELDS:
            raise ValueError(f"Invalid CSV line: {csv_line}")

        abstract, date_modified, image, in_language, name, sections, url = fields

        date_modified = (
            datetime.strptime(date_modified, "%Y-%m-%d %H:%M:%S") if date_modified.lower() != "null" else None
        )

        image_data = (
            ast.literal_eval(image) if image and image.lower() != "null" else None
        )

        if isinstance(image_data, dict):
            image = image_data.get("content_url")

        language_data = (
            ast.literal_eval(in_language) if in_language.lower() != "null" else None
        )

        if isinstance(language_data, dict):
            in_language = language_data.get("identifier")

        return cls(
            abstract=abstract,
            name=name,
            url=url,
            date_modified=date_modified,
            image=image,
            in_language=in_language,
            sections=sections
        )

    def to_list(self):
        return [
            self.abstract,
            self.date_modified.strftime("%Y-%m-%d %H:%M:%S") if self.date_modified else None,
            self.image,
            self.in_language,
            self.name,
            self.sections,
            self.url
        ]