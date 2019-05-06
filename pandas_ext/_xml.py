"""Write a pandas dataframe to xml."""
import pandas as pd
from xml.sax.saxutils import escape


def to_xml(df, filename=None, mode='w'):
    """Write df to xml."""
    def row_to_xml(row):
        """Row by row."""
        xml = ['<item>']
        for i, col_name in enumerate(row.index):
            raw = row.iloc[i]
            if not isinstance(raw, float) and '&' in raw:
                print(raw, type(raw))
            value = escape(raw) if isinstance(raw, str) else raw

            xml.append('  <field name="{0}">{1}</field>'
                       .format(col_name, value)
                       )
        xml.append('</item>')
        return '\n'.join(xml)

    res = '<?xml version="1.0" encoding="UTF-8"?>\n'
    res += '\n'.join(df.apply(row_to_xml, axis=1))

    if filename is None:
        return res
    with open(filename, mode) as f:
        f.write(res)


pd.DataFrame.to_xml = to_xml
