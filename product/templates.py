"""Task templates for the product layer.

Each template turns one user-supplied text item into one chat-completion request.
Templates never see credentials and never choose the upstream URL or model
catalog: the server supplies the model, and BatchForge enforces the rest.
"""
from __future__ import annotations

from dataclasses import dataclass


EXTRACT_FIELDS = ("title", "date", "amount")


@dataclass(frozen=True)
class Template:
    id: str
    label: str
    description: str
    system: str
    max_tokens: int
    json_object: bool = False


TEMPLATES: dict[str, Template] = {
    template.id: template
    for template in (
        Template(
            id="sentiment",
            label="Sentiment",
            description="Label each text as positive, negative or neutral.",
            system=(
                "Classify the sentiment of the user's text. "
                "Reply with exactly one word: positive, negative, or neutral."
            ),
            max_tokens=8,
        ),
        Template(
            id="summary",
            label="Summary",
            description="Summarize each text in at most two sentences.",
            system="Summarize the user's text in at most two sentences. Do not add commentary.",
            max_tokens=160,
        ),
        Template(
            id="extract",
            label="Extract fields",
            description="Pull title, date and amount into a JSON object (null when missing).",
            system=(
                "Extract the fields from the user's text as a JSON object with exactly these keys: "
                + ", ".join(EXTRACT_FIELDS)
                + ". Use null for any field that is not stated. Output only the JSON object."
            ),
            max_tokens=200,
            json_object=True,
        ),
    )
}


def get_template(template_id: str) -> Template:
    try:
        return TEMPLATES[template_id]
    except KeyError:
        raise ValueError(f"Unknown template: {template_id}") from None
