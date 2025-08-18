from textual.theme import Theme

gruvbox_dark = Theme(
    name="gruvbox_dark",
    primary="#85A598",
    secondary="#A89A85",
    warning="#fabd2f",
    error="#fb4934",
    success="#b8bb26",
    accent="#fabd2f",
    foreground="#fbf1c7",
    background="#282828",
    surface="#3c3836",
    panel="#504945",
    dark=True,
    variables={
        "block-cursor-foreground": "#fbf1c7",
        "input-selection-background": "#689d6a40",
    },
)

gruvbox_light = Theme(
    name="gruvbox_light",
    primary="#076678",
    secondary="#427b58",
    warning="#af3a03",
    error="#9d0006",
    success="#79740e",
    accent="#b57614",
    foreground="#3c3836",
    background="#fbf1c7",
    surface="#ebdbb2",
    panel="#d5c4a1",
    dark=False,
    variables={
        "block-cursor-foreground": "#3c3836",
        "input-selection-background": "#a8998440",
    },
)
