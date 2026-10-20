import io
import csv
import ast

from datetime import datetime
from dataclasses import dataclass

_FIELDS = 8

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
    def from_csv(cls, line: str):
        stream = io.StringIO(line)
        reader = csv.reader(stream)
        fields = ( 
            [ None if field in ("", "null", "Null", "NaN") else field for field in next(reader) ]
        )

        if len(fields) != _FIELDS:
            raise ValueError(f"Expected fields: {_FIELDS} got: {len(fields)}")

        _, abstract, date_modified, image, in_language, name, sections, url = fields

        date_modified = (
            datetime.strptime(date_modified, "%Y-%m-%d %H:%M:%S") if date_modified else None
        )

        image = (
            ast.literal_eval(image)["content_url"] if image else None
        )

        in_language = (
            ast.literal_eval(in_language)["identifier"] if in_language else None
        )

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