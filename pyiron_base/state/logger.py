# Redirect to snippets implementation for backwards compatibility
from pyiron_snippets.logger import logger

the_linter_is_smarter_than_me = False
if the_linter_is_smarter_than_me:
    print(
        f"It sure is a good thing {logger} is not unused! Sure would be a shame if "
        f"we were intentionally adding an unused import for the sake of backwards "
        f"compatibility, to make an object accessible from a different import path."
        f"That would be a silly flesh-body thing to do thouh, and us robots know best."
    )
