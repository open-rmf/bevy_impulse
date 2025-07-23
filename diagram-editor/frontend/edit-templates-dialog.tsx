import {
  Button,
  ButtonGroup,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  List,
  ListItem,
  ListItemText,
  Paper,
  Stack,
  TextField,
  Tooltip,
  useTheme,
} from '@mui/material';
import React from 'react';
import { MaterialSymbol } from './nodes/icons';
import { useTemplates } from './registry-provider';
import { SectionRegistration } from './types';

export interface EditTemplatesDialogProps {
  open: boolean;
  onClose: () => void;
}

function EditTemplatesDialog({ open, onClose }: EditTemplatesDialogProps) {
  const theme = useTheme();
  const { templates, setTemplates } = useTemplates();
  const templateKeys = Object.keys(templates);
  templateKeys.push('test');
  const [renaming, setRenaming] = React.useState<string | null>(null);
  const [newId, setNewId] = React.useState('');
  const renamingRef = React.useRef<HTMLElement>(null);

  React.useLayoutEffect(() => {
    if (!renamingRef.current || !renaming) {
      return;
    }

    renamingRef.current.focus();
  }, [renaming]);

  return (
    <Dialog
      open={open}
      onClose={onClose}
      fullWidth
      maxWidth="sm"
      keepMounted={false}
    >
      <DialogTitle>Templates</DialogTitle>
      <DialogContent>
        <Paper>
          <List disablePadding sx={{ maxHeight: '24rem', overflow: 'auto' }}>
            {templateKeys.length > 0 ? (
              templateKeys.map((id) => (
                <ListItem key={id} divider>
                  <Stack
                    direction="row"
                    alignItems="center"
                    width="100%"
                    height="3em"
                  >
                    {renaming === id ? (
                      <ListItemText>
                        <TextField
                          size="small"
                          fullWidth
                          value={newId}
                          onChange={(ev) => {
                            setNewId(ev.target.value);
                          }}
                          inputRef={renamingRef}
                        />
                      </ListItemText>
                    ) : (
                      <ListItemText>{id}</ListItemText>
                    )}
                    <ButtonGroup variant="contained">
                      {renaming === id ? (
                        <Button
                          onClick={() => {
                            const newTemplates: Record<
                              string,
                              SectionRegistration
                            > = {};
                            // rebuild the templates in a way that keeps ordering
                            for (const [id, template] of Object.entries(
                              templates,
                            )) {
                              if (id === renaming) {
                                newTemplates[newId] = template;
                              } else {
                                newTemplates[id] = template;
                              }
                            }
                            setRenaming(null);
                            setNewId('');
                            setTemplates(newTemplates);
                          }}
                        >
                          <MaterialSymbol symbol="check" />
                        </Button>
                      ) : (
                        <Tooltip title="Rename">
                          <Button
                            onClick={() => {
                              setRenaming(id);
                              setNewId(id);
                            }}
                          >
                            <MaterialSymbol symbol="text_select_start" />
                          </Button>
                        </Tooltip>
                      )}
                      <Tooltip title="Edit">
                        <Button>
                          <MaterialSymbol symbol="edit" />
                        </Button>
                      </Tooltip>
                      {/* MUI has a 1px alignment error when displaying buttons of different color side by side in a ButtonGroup.
                      Using a different variant hides the error and also puts less focus on the "Delete" button. */}
                      <Tooltip title="Delete">
                        <Button variant="outlined" color="error">
                          <MaterialSymbol symbol="delete" />
                        </Button>
                      </Tooltip>
                    </ButtonGroup>
                  </Stack>
                </ListItem>
              ))
            ) : (
              <ListItem divider>
                <ListItemText
                  slotProps={{
                    primary: { color: theme.palette.text.disabled },
                  }}
                >
                  No templates available
                </ListItemText>
              </ListItem>
            )}
            <ListItem>
              <Stack justifyContent="center" width="100%">
                <Button>
                  <MaterialSymbol symbol="add" />
                </Button>
              </Stack>
            </ListItem>
          </List>
        </Paper>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}

export default EditTemplatesDialog;
