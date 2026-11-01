from datetime import datetime
from dataclasses import dataclass

@dataclass
class WikipediaArticle:
    abstract: str | None = None
    name: str | None = None
    url: str | None = None
    date_modified: str | datetime | None = None
    image: str | None = None
    in_language: str | None = None
    sections: str | None = None

    def __post_init__(self):
        if isinstance(self.date_modified, str):
            self.date_modified = datetime.strptime(self.date_modified, "%Y-%m-%d %H:%M:%S")
    
    def __eq__(self, other) -> bool:
        if isinstance(other, int):
            return self.date_modified.year == other if self.date_modified else False

        return NotImplemented

    def __lt__(self, other) -> bool:
        if isinstance(other, int):
            return self.date_modified.year < other if self.date_modified else False

        return NotImplemented

    def __le__(self, other) -> bool:
        if isinstance(other, int):
            return self.date_modified.year <= other if self.date_modified else False

        return NotImplemented

    def to_list(self, **kwargs):
        """
        Returns a list containing only the values of the fields explicity
        requested via keyword arguments.

        kwargs
            Keys that correspond to the attributes to include (e.g., name=True, url=True).
            Only keys passed with a thruty value will be included in the final list.
        """
        
        fields = {
            "abstract": self.abstract,
            "name": self.name,
            "url": self.url,
            "date_modified": self.date_modified.strftime("%Y-%m-%d %H:%M:%S") if self.date_modified else None,
            "image": self.image,
            "in_language": self.in_language,
            "sections": self.sections,
        }

        return [value for field, value in fields.items() if kwargs.get(field)]

    def is_language(self, language: str):
        if self.in_language:
            return self.in_language == language

        return False

    def section_contains(self, word: str):
        import json
        
        if not self.sections:
            return False

        sections = json.loads(self.sections)

        return any(
            word.lower() in section.get("name", "").lower()
            for section in sections
        )