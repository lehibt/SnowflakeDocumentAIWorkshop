# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as f
from snowflake.snowpark import DataFrame

# Title of page
st.title("Data extraction check")
st.subheader("Verify the data extracted from the documents in the pipeline via the Document AI model.")
st.write(
    """This is a mini **Streamlit** application to easily
    verify the extracted information from the uploaded Documents
    that have been parsed in a pipeline using the Document AI model.
    """
)

# Get the current credentials
session = get_active_session()

# Get the table the inference results are stored in
source_table = session.table("doc_ai_db.doc_ai_schema.ml_patents_headers")
df_source = source_table.sort(f.col("inserted_dttm"), ascending=True)

# Let the user input values for filtering data to be checked by confidence score
st.divider()
st.subheader("Score thresholds")
st.markdown("Set threshold values for confidence scores below which data should be checked again.")
col1, col2 = st.columns(2)
with col1:
    thresh_application_number_score = st.number_input(
        label="Application number",
        min_value=0.0,
        max_value=1.0,
        step=0.1,
        value=0.9
    )
with col2:
    thresh_invention_title_score = st.number_input(
        label="Invention title",
        min_value=0.0,
        max_value=1.0,
        step=0.1,
        value=0.9
    )

# Filter data frame based on user input for confidence thresholds
df_source_to_check = df_source.filter(
    ((f.col("application_number_score") <= thresh_application_number_score) | (f.col("invention_title_score") <= thresh_invention_title_score))
    & (f.col("checked") == False)
)
df_source_not_to_check = df_source.filter(
    (f.col("application_number_score") > thresh_application_number_score) & (f.col("invention_title_score") > thresh_invention_title_score)
)

# Let user edit (correct) the data
st.divider()
st.subheader("Check data")
st.write("This is the data to be checked (as it comes out of the model).")
df_pd_edited = st.data_editor(df_source_to_check)

# Convert back to Snowpark DataFrame and filter for checked data, i.e. rows that have been marked as checked
df_edited_checked = session.create_dataframe(df_pd_edited).filter(f.col("Checked"))

# Update data on clicking the button
if st.button("Update data."):

    # Merge: Update/delete checked rows from source table
    source_table.merge(
        df_edited_checked,
        source_table["file_name"] == df_edited_checked["file_name"],
        [f.when_matched().update({"checked": df_edited_checked["checked"]})]
        #[f.when_matched().delete()]
    )

    # Write checked data back to table
    df_edited_checked.write.mode("append").save_as_table("doc_ai_db.doc_ai_schema.ml_patents_headers_checked")

    # Show the checked table
    st.divider()
    st.write("This is the checked data.")
    df_processed_show = session.table("doc_ai_db.doc_ai_schema.ml_patents_headers_checked").sort(f.col("inserted_dttm"), ascending=False)
    st.dataframe(df_processed_show)
