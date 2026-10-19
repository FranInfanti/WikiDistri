import ast

from datetime import datetime

_FIELDS = 7

class WikipediaArticle:

    @classmethod
    def from_csv(cls, csv_line: str):
        fields = csv_line.strip().split(",")

        if len(fields) != _FIELDS:
            raise ValueError(f"Invalid CSV line: {csv_line}")

        abstract = fields[0]

        if fields[1] is not None:
            date_modified = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")

        if fields[2] is not None:
            image = ast.literal_eval(fields[2])['content_url']

        if fields[3] is not None:
            in_language = ast.literal_eval(fields[3])['identifier']

        name = fields[4]
        sections = fields[5]
        url = fields[6]

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