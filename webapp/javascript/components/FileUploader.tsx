/* eslint-disable react/jsx-props-no-spreading */
import React, { useCallback } from 'react';
import { useDispatch } from 'react-redux';
import { useDropzone } from 'react-dropzone';
import { faTrash } from '@fortawesome/free-solid-svg-icons/faTrash';
import Button from '@ui/Button';
import { addNotification } from '../redux/reducers/notifications';
import styles from './FileUploader.module.scss';

import { Tab, Tabs, TabList, TabPanel } from 'react-tabs';
import 'react-tabs/style/react-tabs.css';

interface Props {
  onUpload: (s: string) => void;
  file: File;
  setFile: (file: File, flamebearer: Record<string, unknown>) => void;
}
export default function FileUploader({ file, setFile }: Props) {
  const dispatch = useDispatch();

  const onDrop = useCallback((acceptedFiles) => {
    if (acceptedFiles.length > 1) {
      throw new Error('Only a single file at a time is accepted.');
    }

    acceptedFiles.forEach((file) => {
      const reader = new FileReader();

      reader.onabort = () => console.log('file reading was aborted');
      reader.onerror = () => console.log('file reading has failed');
      reader.onload = () => {
        const binaryStr = reader.result;

        try {
          // ArrayBuffer -> JSON
          const s = JSON.parse(
            String.fromCharCode.apply(null, new Uint8Array(binaryStr))
          );
          // Only check for flamebearer fields, the rest of the file format is checked on decoding.
          const fields = ['names', 'levels', 'numTicks', 'maxSelf'];
          fields.forEach((field) => {
            if (!(field in s.flamebearer))
              throw new Error(
                `Unable to parse uploaded file: field ${field} missing`
              );
          });
          setFile(file, s);
        } catch (e) {
          dispatch(
            addNotification({
              message: e.message,
              type: 'danger',
              dismiss: {
                duration: 0,
                showIcon: true,
              },
            })
          );
        }
      };
      reader.readAsArrayBuffer(file);
    });
  }, []);

  const { getRootProps, getInputProps } = useDropzone({
    accept: 'application/json',
    multiple: false,
    noClick: true,
    onDrop
  });

  const onRemove = () => {
    setFile(null, null);
  };

  const exampleFileNames = ['test.py', 'test.json', 'test.js', 'test.ts', 'test.tsx', 'test.jsx'];
  const exampleSizes = ['1.5 KB', '1.5 MB', '1.5 GB', '1.5 TB', '1.5 PB', '1.5 EB'];
  
  // console.dir(styles);
  return (
    <section className={styles.container}>

              <Tabs>
    <TabList>
      <Tab>$PYROSCOPE_DIR</Tab>
      <Tab>Upload</Tab>
      <Tab disabled>Disabled example</Tab>
    </TabList>

    <TabPanel>
      <div className={styles.tableHeaderContainer}>
      <table className="flamegraph-table" data-testid="table-view">
                <thead>
                  <tr>
                    <th>
                      Filename
                    </th>
                    <th>
                      Size
                    </th>
                  </tr>
                </thead>
      </table>
      </div>
      <div className={styles.tableBodyContainer}>
        <table className="flamegraph-table" data-testid="table-view">
                <tbody>
                  {exampleFileNames.map((name, i) => ( <tr key={i}><td>{name}</td><td>{exampleSizes[i]}</td></tr>))}
                  {/* <td>
                    test.py
                  </td>
                  <td>
                    1.2 MB
                  </td> */}
                </tbody>
              </table>
      </div>
              <div className={styles.buttonRow}>
                <Button> Open </Button>
              </div>
    </TabPanel>
    <TabPanel>
    <div className={styles.dragDropZone} {...getRootProps()}>
        <input {...getInputProps()} />
        <div className={styles.instructionText}>
          {file ? (
            <p>
              To analyze another file, drag and drop pyroscope JSON files here or
              click to select a file
            </p>
          ) : (
            <div>
              <p>
                Drag a Pyroscope JSON file here
              </p>

              <Button> 
                Select File 
              </Button>
            </div>
          )}
        </div>
      </div>
      {file && (
        <aside>
          Currently analyzing file {file.path}
          &nbsp;
          <Button icon={faTrash} onClick={onRemove}>
            Remove
          </Button>
        </aside>
      )}
    </TabPanel>
    <TabPanel>
      <p>Disabled tab</p>
    </TabPanel>
  </Tabs>
    </section>
  );
}
